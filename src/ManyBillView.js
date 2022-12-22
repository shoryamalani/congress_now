import React, { useState } from 'react';
import { BrowserRouter, Link, Switch, Route } from 'react-router-dom';
import { StyledEngineProvider } from '@mui/material/styles';
// import logo from './logo.svg';
import './App.css';
import BillView from './BillView';
import { Box } from '@material-ui/core';

import Grid from '@mui/material/Unstable_Grid2';

export default function ManyBillView(props) {
    console.log(props)
    return (
        <Box sx={{mx:'auto'}}>
      <Grid container rowSpacing={10}  columnSpacing={{ xs: 12, sm: 4, md: 12 }} sx={{px:3}}>
        { props.currentBills['results'].map((bill) => (
          <Grid xs={12} sm={12} md={6} key={bill['slug']} sx={{px:2}}>
            <BillView bill={bill} />
          </Grid>
        ) )}
      </Grid>
      </Box>
    )
}