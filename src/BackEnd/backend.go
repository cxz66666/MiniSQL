package BackEnd

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"log"
	"minisql/src/Utils/Error"
	"minisql/src/API"
	"time"
	"strings"
	"minisql/src/BufferManager"
	"minisql/src/Interpreter/parser"
	"minisql/src/Interpreter/types"

)

func Cors() gin.HandlerFunc {
    return func(c *gin.Context) {
        method := c.Request.Method
        origin := c.Request.Header.Get("Origin") 
        if origin != "" {

            c.Header("Access-Control-Allow-Origin", "*") 

            c.Header("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE,UPDATE") 

            c.Header("Access-Control-Allow-Headers", "Authorization, Content-Length, X-CSRF-Token, Token,session")

            c.Header("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers") 

            c.Header("Access-Control-Max-Age", "172800") 

            c.Header("Access-Control-Allow-Credentials", "true")         
                                                                                                                                                                                                          
        }

        if method == "OPTIONS" {
            c.AbortWithStatus(http.StatusNoContent)
        }

        defer func() {
            if err := recover(); err != nil {
                log.Printf("Panic info is: %v", err)
            }
        }()

        c.Next()
    }
}


func handleInstruction(instruction string) (Error.Error, time.Duration ){
	StatementChannel:=make(chan types.DStatements,500)  //用于传输操作指令通道
	FinishChannel:=make(chan Error.Error,500) //用于api执行完成反馈通道
	FlushChannel:=make(chan struct{}) //用于每条指令结束后协程flush
	go API.HandleOneParse(StatementChannel,FinishChannel)  //begin the runtime for exec
	go BufferManager.BeginBlockFlush(FlushChannel)

	beginTime:=time.Now()
	parser.Parse(strings.NewReader(instruction),StatementChannel)

	data := <-FinishChannel //等待指令执行完成
	durationTime:=time.Since(beginTime)
	FlushChannel<- struct{}{} 
	
	return data, durationTime
}
func Regist()  {
	router := gin.Default()
	//router.Use(Cors())
	router.POST("/api/query", func(ctx *gin.Context) {
		res, time := handleInstruction(ctx.PostForm("query"))
		ctx.JSON(200, gin.H{
			"status" : res.Status,
		"time" : time,
		"rows" : res.Rows,
		"data" : res.Data})
	})
	//router.POST("api/login") {

	//}

	router.Run()
}