import React, {useState,useRef} from "react";
import AceEditor from "react-ace";
import {Button, Empty, Layout, Menu, message, PageHeader} from 'antd';
import Axios from "axios";
import Callback from "./Callback";

import 'ace-builds/src-noconflict/ext-language_tools';
import 'ace-builds/src-noconflict/ext-searchbox';
import 'ace-builds/src-noconflict/mode-mysql';
// theme
import 'ace-builds/src-noconflict/theme-sqlserver';
import 'ace-builds/src-noconflict/theme-github';
import 'ace-builds/src-noconflict/theme-eclipse';
import 'ace-builds/src-noconflict/theme-monokai';
import 'ace-builds/src-noconflict/theme-clouds';
import 'ace-builds/src-noconflict/theme-chrome';
import 'ace-builds/src-noconflict/theme-solarized_dark';
import 'ace-builds/src-noconflict/theme-solarized_light';
import Redirect from "react-router-dom/es/Redirect";

function MiniSQL(props) {
    const {userName} = props


    const {SubMenu} = Menu;
    const {Content, Footer, Sider} = Layout;

    const themeList = ["sqlserver", "github", "eclipse", "monokai", "clouds", "chrome", "solarized_dark", "solarized_light"]
    const [theme, setTheme] = useState(themeList[0])
    const EditorRef = useRef()
    const [queryData, setQueryData] = useState([])
    const [checkOn,setCheckOn] = useState()
    const sqlSplit = (texts) => {
        const dtFilter = require("dt-sql-parser").filter;

        const afterFilterComments = dtFilter.filterComments(texts)
        const afterSplit = dtFilter.splitSql(afterFilterComments)
        console.log(afterFilterComments)
        console.log(afterSplit)
        let res = []
        for (let i = 0; i < afterSplit.length; i++) {
            const item = afterSplit[i]
            if (item !== "" && item !== '\n' && item !== undefined) {
                res.push(item.replace(/[\r\n]/g, '').replace('undefined', ''))
            }
        }
        console.log(res)
        return res
    }
    const syntaxCheck = (text) => {
        // if(checkOn===undefined||checkOn===false){
        //     return false
        // }

        const dtSqlParser=require("dt-sql-parser").parser;
        return dtSqlParser.parseSyntax(text);
    }


    const doQuery = (data) => {

        if (userName === 'manager') {
            if (data.indexOf('delete') !== -1 || data.indexOf('drop') !== -1) {
                message.error('权限不足！只有root账号才能进行delete和drop操作')
                return
            }
        } else if (userName === 'customer') {
            console.log(data)
            if (data.indexOf('select') === -1) {
                message.error('权限不足！普通用户只能使用select操作')
                return
            }
        }
        const query = async (data) => {
            try {
                const res = await Axios(
                    'api/query',
                    {
                        method: 'POST',
                        data: {
                            'query': data
                        }
                    }
                );
                setQueryData(res.data.data)
            } catch (e) {
                message.error('后端提示：啊欧，失败了！可能是你的语句不太正常。')
            }
        }

        let texts = sqlSplit(data)
        if (texts === undefined || texts === null) {
            message.error('空代码可跑不了哦！')
            return
        }

        for (let i = 0; i < texts.length; i++) {
            const check = syntaxCheck(texts[i]);
            console.log(check)
            if(check!==false){
                message.error(
                    `前端语法检查:\n
                                错误类型: ${check.token}\n
                                错误位置: \n
                                        开始行数: ${check.loc.first_line}    结束行数: ${check.loc.last_line}\n          
                                        开始列数: ${check.loc.first_column}  结束列数: ${check.loc.first_column}\n          
                                修改建议: \n
                                        改为: ${check.expected!==null&&check.expected.length>0?check.expected[0].text:'暂无'} 
                            `
                )
                return
            }
            query(texts[i])
        }


    }


    return (<div>
        {
            (userName === undefined || userName === null || userName === '') ? <Redirect to="/"/>
                : <Layout>

                    <Content style={{padding: '0 50px'}}>
                        <PageHeader
                            className="site-page-header"
                            title="MinSQL Editor"
                            subTitle={"current user: " + userName}
                        />
                        <Layout className="site-layout-background" style={{padding: '24px 0'}}>
                            <Sider className="site-layout-background" width={200}>
                                <Menu
                                    mode="inline"
                                    style={{height: '100%'}}
                                    onClick={(param) => {
                                        setTheme(themeList[param["key"]])
                                    }}
                                >
                                    <Button
                                        type="primary"
                                        style={{
                                            textAlign: "center",
                                            width: "100%",
                                            marginBottom: "5px"
                                        }}
                                        onClick={(e) => {
                                            e.preventDefault();
                                            const context = EditorRef.current.editor.getValue()
                                            doQuery(context)
                                            EditorRef.current.editor.setValue(context)
                                        }}
                                        ghost
                                    >

                                        Run Code
                                    </Button>
                                    <SubMenu key="theme" title="Theme">
                                        <Menu.Item key={0}>sql server</Menu.Item>
                                        <Menu.Item key={1}>github</Menu.Item>
                                        <Menu.Item key={2}>eclipse</Menu.Item>
                                        <Menu.Item key={3}>monokai</Menu.Item>
                                        <Menu.Item key={4}>clouds</Menu.Item>
                                        <Menu.Item key={5}>chrome</Menu.Item>
                                        <Menu.Item key={6}>solarized_dark</Menu.Item>
                                        <Menu.Item key={7}>solarized_light</Menu.Item>
                                    </SubMenu>
                                </Menu>

                            </Sider>
                            <Content style={{padding: '0 24px', minHeight: 300}}>
                                <AceEditor
                                    ref={EditorRef}
                                    mode="mysql"
                                    theme={theme}
                                    fontSize={16}
                                    style={{
                                        width: '100%',
                                        height: '100%',
                                        minHeight: 300,
                                        fontFamily: "Fira Code, Consolas, monospace"
                                    }}
                                    setOptions={{
                                        enableBasicAutocompletion: false, //关闭基本自动完成功能
                                        enableLiveAutocompletion: true,   //启用实时自动完成功能
                                        enableSnippets: true,
                                        showLineNumbers: true,
                                        editorProps: {$blockScrolling: true},
                                        highlightActiveLine: true,
                                        tabSize: 4
                                    }}
                                />

                            </Content>
                        </Layout>
                        {queryData === null || queryData === undefined
                            ? <Empty/>
                            :
                            <Callback status={queryData[0]} times={queryData[1]} rows={queryData[2]} data={queryData[3]}/>}

                    </Content>
                    <Footer style={{textAlign: 'center'}}>MiniSQL ©2021 Created by Wolfram</Footer>
                </Layout>
        }
    </div>)

}

export default MiniSQL;