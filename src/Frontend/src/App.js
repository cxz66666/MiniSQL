import React, {useState} from 'react';
import MiniSQL from "./MiniSQL/MiniSQL";
import 'antd/dist/antd.css'; // or 'antd/dist/antd.less'
import {BrowserRouter as Router, Route, Switch} from 'react-router-dom'
import Login from './Login/Login'
import './App.css'
import {Empty} from "antd";

// function App() {
//     const [userName, setUserName] = useState('')
//     return (
//         <Router>
//             <Switch>
                
//                 <Route exact path="/">
//                     <div id='login-minisql'>
//                         <Login userName={userName} setUserName={setUserName}/>
//                     </div>
//                 </Route>
                
//                 <Route path="/query">
//                     <MiniSQL userName={userName}/>
//                 </Route>
//                 <Route path="*">
//                     <Empty/>
//                 </Route>
//             </Switch>
//         </Router>
//     );
// }

function App() {
    const [userName, setUserName] = useState('')
    return (
        <Router>
            <Switch>
                
                <Route exact path="/">
                    <MiniSQL userName="root"/>
                </Route>
                
                <Route path="/query">
                <Empty/>
                </Route>
                <Route path="*">
                    
                </Route>
            </Switch>
        </Router>
    );
}

export default App;
