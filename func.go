package pool

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/jinzhu/copier"
	"github.com/rs/xid"
)

func CurrentTime() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
func CurrentTimeNumberFormat() string {
	return time.Now().Format("20060102150405")
}

//FormatError 输出格式化错误信息
func FormatError(httpCode, businessCode int, msg string) (err error) {
	err = fmt.Errorf("%d:%d:%s", httpCode, businessCode, msg)
	return
}

// 判断所给路径文件/文件夹是否存在(返回true是存在)
func IsExist(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

//调用os.MkdirAll递归创建文件夹
func Mkdir(filePath string) error {
	if !IsExist(filePath) {
		err := os.MkdirAll(filePath, os.ModePerm)
		return err
	}
	return nil
}

func Copy(src, dst string) (int64, error) {
	dstDir := path.Dir(dst)
	Mkdir(dstDir)
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}

	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

func GetMD5LOWER(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

// 生成随机ID
func Xid() string {
	guid := xid.New()
	return guid.String()
}

// 布尔值转整数
func Bool2Int(boolean bool) int {
	boolMap := map[bool]int{true: 1, false: 2}
	return boolMap[boolean]
}

const (
	BOOLEAN_TRUE  = "true"
	BOOLEAN_FALSE = "false"
)

// 布尔值转字符
func Bool2Str(val bool) string {
	output := BOOLEAN_FALSE
	if val {
		output = BOOLEAN_TRUE
	}
	return output
}

func Str2Bool(s string) bool {
	output := false
	if s == BOOLEAN_TRUE {
		output = true
	}
	return output
}

//ConvertStruct 转换结构体
func ConvertStruct(from interface{}, to interface{}) {
	err := copier.Copy(to, from)
	if err != nil {
		panic(err)
	}
}
