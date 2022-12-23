import * as React from 'react';
import './App.css';
import {useState,useEffect} from 'react';
import OutlinedInput from '@mui/material/OutlinedInput';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import ListItemText from '@mui/material/ListItemText';
import Select from '@mui/material/Select';
import Checkbox from '@mui/material/Checkbox';
import { ThemeProvider, createTheme,useTheme } from '@mui/material/styles';
import { TextField } from '@mui/material';
import { styled } from '@mui/material/styles';
import MuiGrid from '@mui/material/Grid';
import Divider from '@mui/material/Divider';
import { StyledEngineProvider } from '@mui/material/styles';
import { LoadingButton } from '@mui/lab';
import ManyBillView from './ManyBillView';
const Grid = styled(MuiGrid)(({ theme }) => ({
  width: '100%',
  '& [role="separator"]': {
    margin: theme.spacing(0, 1),
  },
}));
const ITEM_HEIGHT = 48;
const ITEM_PADDING_TOP = 8;
const MenuProps = {
  PaperProps: {
    // style: {
    //   maxHeight: ITEM_HEIGHT * 4.5 + ITEM_PADDING_TOP,
    //   width: 250,
    // },
  },
};

const names = [
  'Oliver Hansen',
  'Van Henry',
  'April Tucker',
  'Ralph Hubbard',
  'Omar Alexander',
  'Carlos Abbott',
  'Miriam Wagner',
  'Bradley Wilkerson',
  'Virginia Andrews',
  'Kelly Snyder',
];

const darkTheme = createTheme({
  palette: {
    mode: 'dark',
  },
});
// var style = StyleSheet.create({
//   search_types_div: {
//     flex: 1,
//     justifyContent: 'center',
//     alignItems: 'center',
//     flexDirection: 'row',
//   },
// });
export default function BillSearch(props) {
    // Make a search for congressional bills using MUI
    // https://mui.com/components/selects/#multiple-select
    // https://mui.com/components/selects/#checkboxes
    const [bills, setBills] = useState({results: []});
    function searchBills(){
      console.log("searching")
      setLoading(true);
      fetch('/api/bill_search').then(res => res.json()).then(data => {
        console.log(data)
        setBills({"results":data});
      });
      setLoading(false);
    }
    function searchBillsText(){
      console.log("searching")
      setLoading(true);
      // Post the search text to the server
      fetch('/api/bill_search_text', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({search_text: search_text}),
      }).then(res => res.json()).then(data => {
        console.log(data)
        setBills({"results":data});
      });
      setLoading(false);
    }
    const [loading_val, setLoading] = useState(false);
    const [personName, setPersonName] = React.useState([]);
    const [search_text, setSearchText] = React.useState('');
    const handleChange = (event) => {
      const {
        target: { value },
      } = event;
      setPersonName(
        // On autofill we get a stringified value.
        typeof value === 'string' ? value.split(',') : value,
      );
    };
    var handleOnChangeText = event => {
      setSearchText(event.target.value)
    };
  
    return (
      <StyledEngineProvider injectFirst>
      <ThemeProvider theme={darkTheme}>
      <div className='search_types_div'>
      
      {/* <Divider orientation="vertical" flexItem></Divider> */}
      
      </div>
      {/* <Grid container> */}
        {/* <Grid item xs>
        <div>
      <p>
          Search By Subject
        </p>
        <FormControl sx={{ m: 1, width: 300 }}>
          <InputLabel id="demo-multiple-checkbox-label">Subject</InputLabel>
          <Select
          inputProps={{
            MenuProps: {
                MenuListProps: {
                    sx: {
                        backgroundColor: 'black'
                    }
                }
            }
        }} */}
            {/* labelId="demo-multiple-checkbox-label"
            id="demo-multiple-checkbox"
            // multiple
            value={personName}
            onChange={handleChange}
            input={<OutlinedInput label="Tag" />}
            renderValue={(selected) => selected.join(', ')}
            MenuProps={MenuProps}
          > */}
            {/* {names.map((name) => (
              <MenuItem key={name} value={name}>
                <Checkbox checked={personName.indexOf(name) > -1} />
                <ListItemText primary={name} />
              </MenuItem>
            ))}
          </Select>
        </FormControl><br></br>
        <LoadingButton loading={loading_val} loadingIndicator="Loading…" variant="outlined" onClick={searchBills}>Fetch data</LoadingButton>
      </div>
        </Grid> */}
        {/* <Divider orientation="vertical" flexItem>
        </Divider> */}
        <Grid item xs>
        <div>
        <p>
          Search By Text
        </p>
        <TextField id="outlined-search" label="Search field" type="search" onChange={handleOnChangeText} /><br></br>
        <LoadingButton loading={loading_val} loadingIndicator="Loading…" variant="outlined" onClick={searchBillsText}>Fetch data</LoadingButton>
      </div>
        </Grid>
      {/* </Grid> */}
      </ThemeProvider>
      <ManyBillView currentBills={bills}></ManyBillView>
      </StyledEngineProvider>

    );
}