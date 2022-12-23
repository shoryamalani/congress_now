import * as React from 'react';
import { styled } from '@mui/material/styles';
import Card from '@mui/material/Card';
import CardHeader from '@mui/material/CardHeader';
import CardMedia from '@mui/material/CardMedia';
import CardContent from '@mui/material/CardContent';
import CardActions from '@mui/material/CardActions';
import Collapse from '@mui/material/Collapse';
import Avatar from '@mui/material/Avatar';
import IconButton, { IconButtonProps } from '@mui/material/IconButton';
import Typography from '@mui/material/Typography';
import { red } from '@mui/material/colors';
import FavoriteIcon from '@mui/icons-material/Favorite';
import ShareIcon from '@mui/icons-material/Share';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import { Paper } from '@mui/material';
import Stack from '@mui/material/Stack';
import './App.css';
import { StyledEngineProvider } from '@mui/material/styles';
import { TwitterTimelineEmbed, TwitterShareButton, TwitterFollowButton, TwitterHashtagButton, TwitterMentionButton, TwitterTweetEmbed, TwitterMomentShare, TwitterDMButton, TwitterVideoEmbed, TwitterOnAirButton } from 'react-twitter-embed';
const ExpandMore = styled((props) => {
  const { expand, ...other } = props;
  return <IconButton {...other} />;
})(({ theme, expand }) => ({
  transform: !expand ? 'rotate(0deg)' : 'rotate(180deg)',
  marginLeft: 'auto',
  padding: theme.spacing(1),
  transition: theme.transitions.create('transform', {
    duration: theme.transitions.duration.shortest,
  }),
}));

export default function BillView(props) {
  const [expanded, setExpanded] = React.useState(false);
  // console.log(props)
  props = props['bill']
  if (props === undefined) {
    return (<div></div>)
  }
  const handleExpandClick = () => {
    setExpanded(!expanded);
  };
  var color = "gray"
  if (props['sponsorParty'] === 'R'){
    color = 'red'
  }else{
    color = 'blue'
  }
  return (
    <StyledEngineProvider injectFirst>
    <Card variant='outlined' sx={{ flexGrow:'auto' }}>
      <CardHeader
        avatar={ 
          <Avatar sx={{ bgcolor: color, color:"white" }}  aria-label="recipe">
            {props['sponsorParty']} 
          </Avatar>
        }
        action={
          <IconButton aria-label="settings">
            <MoreVertIcon />
          </IconButton>
        }
        title= {props['name']}
        subheader={props['sponsor']}
      />
      {/* Center this avatar */}
      <CardContent style={
        {display: 'flex', justifyContent: 'center', alignItems: 'center',}
      }>
      <Avatar alt={props["name"]} src={props['photo']} sx={{ width: 130,height:130,px:2}} ></Avatar>
      </CardContent>
      {/* <CardMedia
        component="img"
        flexGrow="unset"
        style={{width: 200, justifySelf: 'center', alignSelf: 'center', margin: 1,}}
        width="200"
        src={props['photo']}
        alt="Congress person photo"
      /> */}
      <CardContent >
        <Typography variant="body2" color="text.primary">
          <a href={props['url']} >{props['url']}</a>
        </Typography>
        <Typography variant="body1" color="text.primary">
          {props['committees']}
        </Typography>
      </CardContent>
      <CardContent>
        <Typography variant="body2" color="text.secondary">
          introduced on: {props['introducedDate']}
        </Typography>
        
        <Typography variant="body2" color="text.primary">
          update on {props['lastActionDate']}: {props['lastAction']}
        </Typography>
        <Typography variant="body2" color="text.primary">
          Number of cosponsors: {props['cosponsors']}
        </Typography>
        <Typography variant="body2" color="text.primary">
          <span> Republicans: {props['cosponsors_by_party']['R']}</span>
           <span> Democrats: {props['cosponsors_by_party']['D']}</span>
        </Typography>
        <Typography variant="body2" color="text.primary">
          <span> Primary Subject: {props['primarySubject']}</span>
        </Typography>
        <Typography variant="body2" color="text.primary">
          <span> GovTrack: <a href={props['govtrack']}>{props['govtrack']} </a></span>
        </Typography>

      </CardContent>
      <CardContent>
        <Typography variant="body2" color="text.secondary">
          {props['summary']} 

        </Typography>
      </CardContent>
      <CardActions disableSpacing>
        <IconButton aria-label="add to favorites">
          <FavoriteIcon />
        </IconButton>
        <IconButton aria-label="share">
          {/* <ShareIcon />
        </IconButton>
        <IconButton aria-label='share-twitter'> */}
        <TwitterShareButton
        // url should be the url of the website
    url={props['congress_now_url']}
    options={{ text: 'About: ' + props['name'] + ' ' +(props['twitter_account'] != null ? "@"+props['twitter_account'] :"")  , via:'shoryamalani' ,size:'large' }}
  />
        </IconButton>
        <ExpandMore
          expand={expanded}
          onClick={handleExpandClick}
          aria-expanded={expanded}
          aria-label="show more"
        >
          <ExpandMoreIcon />
        </ExpandMore>
      </CardActions>
      <Collapse in={expanded} timeout="auto" unmountOnExit>
        <CardContent>
          <Typography paragraph>Method:</Typography>
          <Typography paragraph>
            Heat 1/2 cup of the broth in a pot until simmering, add saffron and set
            aside for 10 minutes.
          </Typography>
        </CardContent>
      </Collapse>
    </Card>
    </StyledEngineProvider>
  );
}
