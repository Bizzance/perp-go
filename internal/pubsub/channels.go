// pubsub包只放Redis channel命名——不放发布/订阅逻辑本身(那些分别在
// internal/service/push.go发布端、internal/ws/hub.go订阅端)。发布端和订阅端各自维护
// 一份"perpgo:ws:"前缀常量/命名函数，是这个功能第一次实测联调时踩到的真实bug：两边前缀
// 对不上，Hub订阅了错误的Redis channel，看起来"连上了但永远收不到推送"，没有任何报错。
// 现在统一收进这一个包，两边都调用同一份函数，结构上不可能再出现前缀不一致
package pubsub

import "strconv"

// Prefix 是WS相关Redis channel的统一前缀，跟这个项目历史上跟一个Java版本共用过Redis时
// 遗留的其它key/channel(比如contract:*)做命名隔离
const Prefix = "perpgo:ws:"

func DepthChannel(symbol string) string     { return Prefix + "depth:" + symbol }
func TradeChannel(symbol string) string     { return Prefix + "trade:" + symbol }
func MarkPriceChannel(symbol string) string { return Prefix + "markprice:" + symbol }
func KlineChannel(symbol, interval string) string {
	return Prefix + "kline:" + symbol + ":" + interval
}
func UserChannel(uid uint64) string { return Prefix + "user:" + strconv.FormatUint(uid, 10) }
