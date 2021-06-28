package BackEnd

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"log"
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

func regist()  {
	router := gin.Default()
	router.Use(Cors())
	router.POST("/api/query", func(ctx *gin.Context) {
		
	})
	//router.POST("api/login") {

	//}

	router.Run()
}