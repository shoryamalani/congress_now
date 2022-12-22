import React, { useState, useEffect,Text } from 'react';
import { BrowserRouter, Link, Switch, Route } from 'react-router-dom';
import { StyledEngineProvider } from '@mui/material/styles';
// import logo from './logo.svg';
import './App.css';
// import BillView from './BillView';
import NavigationBar from './NavigationBar';
// import Box from '@mui/material/Box';
// import Grid from '@mui/material/Grid'; // Grid version 1
// import Grid from '@mui/material/Unstable_Grid2';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import { spacing } from '@mui/system';
import BillSearch from './BillSearch';
import ManyBillView from './ManyBillView';
import LoadingButton from '@mui/lab/LoadingButton';
import SaveIcon from '@mui/icons-material/Save';
import Stack from '@mui/material/Stack';
import Toolbar from '@mui/material/Toolbar';
const darkTheme = createTheme({
  palette: {
    mode: 'dark',
  },
});
function App() {
  // const [currentTime, setCurrentTime] = useState(0);
  const [bills, setBills] = useState({results: []});
  // const [loading_val, setLoading] = useState(false);
  // useEffect(() => {
  //   fetch('/api/time').then(res => res.json()).then(data => {
  //     setCurrentTime(data.time);
  //   });
  // }, []);
  useEffect(() => {
    fetch('/api/all_bills').then(res => res.json()).then(data => {
      console.log(data)
      setBills({"results":data});
    });
  },{})
  

  return (
    <StyledEngineProvider injectFirst>
    <ThemeProvider theme={darkTheme}>
    <CssBaseline />
    <div className="App">
      <header className="App-header">
        <BrowserRouter>
        <NavigationBar />
        <Toolbar />
          {/* <div>
            <Link className="App-link" to="/">Recent Bills</Link>
            &nbsp;|&nbsp;
            <Link className="App-link" to="/bill_search">Bill Search</Link>
          </div> */}
          <Switch>
            <Route exact path="/">
                {/* <img src={logo} className="App-logo" alt="logo" /> */}
                <p >
                  Lets look at some recent congressional bills...
                </p>
                <ManyBillView currentBills={bills} />
                {/* <a
                  className="App-link"
                  href="https://reactjs.org"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Learn React
                </a> */}

                {/* <p>The current time is {currentTime}.</p> */}
                {/* <Box sx={{mx:'auto'}}>
                <Grid container rowSpacing={10}  columnSpacing={{ xs: 12, sm: 4, md: 12 }} sx={{px:3}}>
                  { bills['results'].map((bill) => (
                    <Grid xs={12} sm={12} md={6} key={bill['slug']} sx={{px:2}}>
                      <BillView bill={bill} />
                    </Grid>
                  ) )}
                </Grid>
                </Box> */}
                
                {/* { bills['results'].map((bill) => (
                  <BillView bill={bill} />
                ))

                } */}
                
            </Route>
            <Route path="/bill_search">
                <p>Search for Bills</p>
                <BillSearch theme={{darkTheme}}></BillSearch>
                

                
            </Route>
          </Switch>
        </BrowserRouter>
      </header>
    </div>
    </ThemeProvider>
    </StyledEngineProvider>
  );
}


export default App;
