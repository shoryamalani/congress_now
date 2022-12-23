import React, { useEffect, useState } from 'react';
import { BrowserRouter, Link, Switch, Route, useParams } from 'react-router-dom';
import { StyledEngineProvider } from '@mui/material/styles';
// import logo from './logo.svg';
import './App.css';
import BillView from './BillView';
// import { Divider } from '@mui/material';
// import { Box } from '@material-ui/core';

// import Grid from '@mui/material/Unstable_Grid2';
// import { getBill } from './App';
export default function SingleBillView(props) {
    const { slug } = useParams();
    // const [bill, setBill] = useState({"name": "Loading", "url": null, "govtrack": "loading...", "sponsor": "loading...", "sponsorId": "loading...", "photo": null, "twitter_account": null, "facebook_account": null, "sponsorParty": "R", "sponsorState": "", "summary": "", "slug": "", "introducedDate": "", "lastActionDate": "", "lastAction": "loading", "votes": [], "lastVoteDate": null, "cosponsors": 0, "cosponsors_by_party": {"D": 0, "R": 0}, "committees": "loading...", "primarySubject": "loading...", "relatedBills": {"count": 0, }});
    const [bill, setBill] = useState(null)
    useEffect(() => {
        console.log(slug)
        getBill(slug)
    }, {})
    function getBill(slug){
        fetch('/api/bill/'+slug).then(res => res.json()).then(data => {
          console.log(data[0])
          setBill(data[0])
        });
      }
    
    
    return (
    //     <Box sx={{mx:'auto'}}>
    //   <Grid container rowSpacing={10}  columnSpacing={{ xs: 12, sm: 4, md: 12 }} sx={{px:3}}>
    //     { props.currentBills['results'].map((bill) => (
    //       <Grid xs={12} sm={12} md={6} key={bill['slug']} sx={{px:2}}>
//     {/* </Grid>
//   ) )}
// </Grid>
// </Box> */}
            <div>
            {bill != null &&
            <>
            <BillView bill={bill} />
            <br />
            </>
            }{
            bill == null &&
            <div>Loading...</div>
            }
            
            </div>
            )
}