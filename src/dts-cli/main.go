package main

import "dts-cli/cmd"

func main() {
    // 调用 Cobra 的 Execute，移交控制权
    cmd.Execute()
}