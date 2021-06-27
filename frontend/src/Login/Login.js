import React, {useEffect, useRef, useState} from 'react';
import {Form, Button, Input, PageHeader, message} from 'antd';
import Axios from "axios";
import Redirect from "react-router-dom/es/Redirect";
import Crypto from 'crypto'

function Login(props) {
    const {setUserName, userName} = props;
    const [form] = Form.useForm();

    const layout = {
        labelCol: {
            span: 8,
        },
        wrapperCol: {
            span: 16,
        },
    };
    const tailLayout = {
        wrapperCol: {
            offset: 8,
            span: 16,
        },
    };

    const doLogin = (values)=>{
        const login = async (data) =>{
            const sha256 = Crypto.createHash('sha256')
            let pwd = sha256.update(data.password+'ko no dio da!').digest('hex')
            console.log(pwd)
            console.log(data.username)
            try {
                const res = await Axios(
                    'api/login',
                    {
                        method:'POST',
                        data:{
                            'username':data.username,
                            'password':pwd
                        }
                    }
                );
                if(res.data.status==="Success"){
                    setUserName(values.username)
                    message.success(`登录成功！欢迎，${values.username}`)
                    return
                }
                message.error('登录失败！请检查一下账号和密码是否正确')
            }catch (e) {
                message.error('登录失败！请检查一下后端是否开启')
            }
        }

        login(values)
    }


    return (<div>{userName!==''&&userName!==undefined&&userName!==null?<Redirect to="/query"/>:<div>
        <PageHeader title={"数据库系统课程设计:MiniSQL 登录"}/>
        <Form
            {...layout}
            form={form}
            name="basic"

        >
            <Form.Item
                label="Username"
                name="username"
                rules={[
                    {
                        required: true,
                        message: 'Please input your username!',
                    },
                ]}
            >
                <Input/>
            </Form.Item>

            <Form.Item
                label="Password"
                name="password"
                rules={[
                    {
                        required: true,
                        message: 'Please input your password!',
                    },
                ]}
            >
                <Input.Password/>
            </Form.Item>

            <Form.Item {...tailLayout}>
                <Button type="primary" htmlType="submit" onClick={()=>{
                    form.validateFields()
                        .then(values => {
                            doLogin(values);
                        }).catch(info => {
                        console.log('Validate Failed:', info);
                    });
                }}>
                    Submit
                </Button>
            </Form.Item>
        </Form>
    </div>}
    </div>)

}

export default Login;