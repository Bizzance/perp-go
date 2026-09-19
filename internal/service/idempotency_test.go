package service

import "testing"

func TestRequestFingerprint_DeterministicAndSensitive(t *testing.T) {
	a := RequestFingerprint("balance", "10001", "100")
	if a != RequestFingerprint("balance", "10001", "100") {
		t.Error("相同参数的指纹必须一致")
	}
	if len(a) != 64 {
		t.Errorf("指纹应该是64位十六进制, got %d位", len(a))
	}
	for _, other := range [][]string{
		{"balance", "10001", "101"},
		{"credit", "10001", "100"},
		{"balance", "10002", "100"},
	} {
		if RequestFingerprint(other...) == a {
			t.Errorf("参数 %v 不同，指纹不应该相同", other)
		}
	}
}

// 参数值里带分隔符不能让不同的参数组合拼出同一个串
func TestRequestFingerprint_NoAmbiguousConcatenation(t *testing.T) {
	if RequestFingerprint("ab", "c") == RequestFingerprint("a", "bc") {
		t.Error("(ab,c)和(a,bc)不应该产生相同指纹")
	}
}
