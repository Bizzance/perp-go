// 集成测试的公共设施：给每个测试一个独立的、按schema.sql建好表的MySQL库，以及一个Redis连接。
// 只给带integration构建标签的测试文件用，不进生产二进制。环境变量由scripts/test-integration.sh
// (make test-integration)拉起一次性的MySQL/Redis容器后设置。
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"perp-go/internal/cache"
	"perp-go/internal/db"
)

const (
	// 管理员DSN，指向MySQL服务但不带库名，需要有建库/删库权限，并且开了multiStatements
	envMySQLAdminDSN = "PERP_TEST_MYSQL_DSN"
	envRedisAddr     = "PERP_TEST_REDIS_ADDR"
	envRedisPass     = "PERP_TEST_REDIS_PASS"
)

// schema.sql开头的建库和切库语句，测试里每个用例用自己的库，要去掉
var dbPreambleRe = regexp.MustCompile(`(?m)^\s*(CREATE DATABASE[^;]*;|USE\s+\w+;)\s*$`)

func requireEnv(t testing.TB, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		// 不用Skip：带integration标签就是明确要跑，环境没配好却显示"通过/跳过"会给人错误的安心
		t.Fatalf("缺少环境变量%s：集成测试需要真实的MySQL和Redis，请用 make test-integration 运行", name)
	}
	return v
}

// 新建一个只属于这个测试的MySQL库(库名随机)，按sql/schema.sql建好全部表和演示种子数据，
// 返回连到这个库的连接(走db.Connect，跟生产用同一套连接设置)。测试结束时自动删库
func NewDB(t testing.TB) *sqlx.DB {
	t.Helper()
	adminDSN := requireEnv(t, envMySQLAdminDSN)
	admin, err := sqlx.Connect("mysql", adminDSN)
	if err != nil {
		t.Fatalf("连接测试MySQL失败: %v", err)
	}
	defer admin.Close()

	name := "perpgo_it_" + randomHex(6)
	if _, err := admin.Exec("CREATE DATABASE " + name + " DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
		t.Fatalf("建测试库失败: %v", err)
	}
	t.Cleanup(func() {
		c, err := sqlx.Connect("mysql", adminDSN)
		if err != nil {
			t.Logf("清理测试库%s时连接失败: %v", name, err)
			return
		}
		defer c.Close()
		if _, err := c.Exec("DROP DATABASE IF EXISTS " + name); err != nil {
			t.Logf("删除测试库%s失败: %v", name, err)
		}
	})

	cfg, err := mysql.ParseDSN(adminDSN)
	if err != nil {
		t.Fatalf("解析%s失败: %v", envMySQLAdminDSN, err)
	}
	cfg.DBName = name
	cfg.MultiStatements = true
	conn, err := db.Connect(cfg.FormatDSN())
	if err != nil {
		t.Fatalf("连接测试库失败: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	if _, err := conn.Exec(loadSchema(t)); err != nil {
		t.Fatalf("执行schema.sql失败: %v", err)
	}
	return conn
}

// 连到测试Redis。Redis没有按测试隔离(键名里带symbol/uid，测试用各自随机的uid就不会撞)，
// 所以make test-integration用-p 1让不同包的测试串行，同一个包里的测试也不要t.Parallel()
func NewCache(t testing.TB) *cache.Cache {
	t.Helper()
	c, err := cache.Connect(requireEnv(t, envRedisAddr), os.Getenv(envRedisPass))
	if err != nil {
		t.Fatalf("连接测试Redis失败: %v", err)
	}
	return c
}

// 直连测试Redis的原始客户端，给测试用来直接读写/删除键(比如清掉某个symbol的标记价格，
// 让"还没有标记价格"这类场景可控)。业务代码走的是NewCache返回的Cache
func NewRedisClient(t testing.TB) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: requireEnv(t, envRedisAddr), Password: os.Getenv(envRedisPass)})
	t.Cleanup(func() { c.Close() })
	return c
}

// 清掉这些symbol跟标记价有关的全部Redis数据(标记价、最新成交价、指数价和它的时间戳)，并且在测试结束时
// 再清一次。这些键按symbol、不按测试隔离：上一个测试留下的指数价会让下一个测试的标记价不再是"最新成交价"
// (指数价存在时标记价由指数价、基差、成交价取中位数得出)，整包一起跑才会出问题、单独跑不会，很难查
func ResetPriceKeys(t testing.TB, symbols ...string) {
	t.Helper()
	clean := func() {
		c := redis.NewClient(&redis.Options{Addr: requireEnv(t, envRedisAddr), Password: os.Getenv(envRedisPass)})
		defer c.Close()
		var keys []string
		for _, s := range symbols {
			keys = append(keys, "perpgo:mark:"+s, "perpgo:last:"+s, "perpgo:index:"+s, "perpgo:index_ts:"+s)
		}
		if err := c.Del(context.Background(), keys...).Err(); err != nil {
			t.Errorf("清理标记价相关的Redis键失败: %v", err)
		}
	}
	clean()
	t.Cleanup(clean)
}

// 每个测试用自己的uid段，避免同一个Redis里不同测试的锁、限流键互相影响。返回一个随机的
// 大uid，同一个测试里的多个账户用它加偏移
func UIDBase() uint64 {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return 1_000_000 + uint64(b[0])<<24 | uint64(b[1])<<16 | uint64(b[2])<<8 | uint64(b[3])
}

func loadSchema(t testing.TB) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(file), "..", "..", "sql", "schema.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取%s失败: %v", path, err)
	}
	return dbPreambleRe.ReplaceAllString(string(raw), "")
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
