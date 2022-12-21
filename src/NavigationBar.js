// https://github.com/dharmelolar/material-UI-navbar/blob/master/src/component/Navbar.js

import React from "react";
import {AppBar,Toolbar,CssBaseline,Typography,makeStyles,useTheme,useMediaQuery,} from "@material-ui/core";
import { Link } from "react-router-dom";
import DrawerComponent from "./Drawer";

const useStyles = makeStyles((theme) => ({
  navlinks: {
    marginLeft: theme.spacing(5),
    display: "flex",
  },
  logo: {
    flexGrow: "1",
    cursor: "pointer",
    
  },
  
  link: {
    textDecoration: "none",
    color: "white !important",
    fontSize: "20px",
    marginLeft: theme.spacing(20),
    "&:visited": {
        color: "#479ff8",
    },
    "&:hover": {
        color: "yellow !important",
        borderBottom: "1px solid white",
    },
    // "&:active": {
    //     color: "white",
    // }

    

    
  },

  AppBar:{
    position: "static",
    display: "flex",
    flexDirection: "row",
    top: 0
}

}));

function Navbar() {
  const classes = useStyles();
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down("md"));

  return (
    <AppBar  >
      <CssBaseline />
      <Toolbar>
        <Typography variant="h4" className={classes.logo}>
          Congress Now
          
        </Typography>
        {isMobile ? (
          <DrawerComponent />
        ) : (
          <div className={classes.navlinks}>
            <Link to="/" className={classes.link}>
              Recent Bills
            </Link>
            <Link to="/bill_search" className={classes.link}>
              Bill Search
            </Link>
            <Link to="/contact" className={classes.link}>
              Contact
            </Link>
            <Link to="/faq" className={classes.link}>
              FAQ
            </Link>
          </div>
        )}
      </Toolbar>
    </AppBar>
  );
}
export default Navbar;