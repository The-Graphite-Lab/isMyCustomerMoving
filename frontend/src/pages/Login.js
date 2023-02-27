import React, { useCallback } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import GoogleButton from 'react-google-button';

// @mui
import { styled } from '@mui/material/styles';
import { Card, Link, Container, Typography } from '@mui/material';

// hooks
import useResponsive from '../hooks/useResponsive';
// components
import Page from '../components/Page';
import Logo from '../components/Logo';

import { LoginForm } from '../sections/auth/login';
// ----------------------------------------------------------------------

const RootStyle = styled('div')(({ theme }) => ({
  [theme.breakpoints.up('md')]: {
    display: 'flex',
  },
}));

const HeaderStyle = styled('header')(({ theme }) => ({
  top: 0,
  zIndex: 9,
  lineHeight: 0,
  width: '100%',
  display: 'flex',
  alignItems: 'center',
  position: 'absolute',
  padding: theme.spacing(3),
  justifyContent: 'space-between',
  [theme.breakpoints.up('md')]: {
    alignItems: 'flex-start',
    padding: theme.spacing(7, 5, 0, 7),
  },
}));

const SectionStyle = styled(Card)(({ theme }) => ({
  width: '100%',
  maxWidth: 464,
  display: 'flex',
  flexDirection: 'column',
  justifyContent: 'center',
  margin: theme.spacing(2, 0, 2, 2),
}));

const ContentStyle = styled('div')(({ theme }) => ({
  maxWidth: 480,
  margin: 'auto',
  minHeight: '100vh',
  display: 'flex',
  justifyContent: 'center',
  flexDirection: 'column',
  padding: theme.spacing(12, 0),
}));

// ----------------------------------------------------------------------

export default function Login() {
  const smUp = useResponsive('up', 'sm');

  const mdUp = useResponsive('up', 'md');

  const { REACT_APP_GOOGLE_CLIENT_ID, REACT_APP_BASE_BACKEND_URL } = process.env;
  console.log(process.env.REACT_APP_GOOGLE_CLIENT_ID)

  const openGoogleLoginPage = useCallback(() => {
    const googleAuthUrl = 'https://accounts.google.com/o/oauth2/v2/auth';
    const redirectUri = 'api/v1/auth/login/google/';

    const scope = [
      'https://www.googleapis.com/auth/userinfo.email',
      'https://www.googleapis.com/auth/userinfo.profile'
    ].join(' ');

    const params = {
      response_type: 'code',
      client_id: REACT_APP_GOOGLE_CLIENT_ID,
      redirect_uri: `${REACT_APP_BASE_BACKEND_URL}/${redirectUri}`,
      prompt: 'select_account',
      access_type: 'offline',
      scope
    };

    const urlParams = new URLSearchParams(params).toString();

    window.location = `${googleAuthUrl}?${urlParams}`;
  }, []);

  return (
    <Page title="Login">
      <RootStyle>
        <HeaderStyle>
          <Logo />

          {smUp && (
            <Typography variant="body2" sx={{ mt: { md: -2 } }}>
              Don’t have an account? {''}
              <Link variant="subtitle2" component={RouterLink} to="/register">
                Get started
              </Link>
            </Typography>
          )}
        </HeaderStyle>

        {mdUp && (
          // "title": "Don't Lose Your Customers,",
          // "subtitle": "Move With Them!",
          // "description": "You don't know when your customers are moving, and they're too busy to remember your name. We instantly notify you when your customers list their home for sale, so you can be the first one to the new home.",
          <SectionStyle>
            <Typography variant="h2" sx={{ px: 5, mt: 10, mb: 5 }}>
              <span >Don't Lose Your Customers,</span>{" "}
              <span style={{color:"#8ce8c5"}}>Move With Them!</span>            
            </Typography>
            <Typography variant="body1" sx={{mx:4}}>We instantly notify you when your customers list their home for sale, so you can be the first one to the new home.</Typography>
            <img src="/static/illustrations/illustration_login.png" alt="login" />
          </SectionStyle>
        )}

        <Container maxWidth="sm">
          <ContentStyle>
            <Typography variant="h2" gutterBottom>
              Sign In
            </Typography>

            <Typography sx={{ color: 'text.secondary', mb: 5 }}>Enter your details below.</Typography>

            <LoginForm />
            {/* create a div that centers the google button and has margin top */}
            <div style={{display: "flex", justifyContent: "center", marginTop: "20px"}}>
              <GoogleButton
                onClick={openGoogleLoginPage}
                label="Login with Google"
                disabled={!REACT_APP_GOOGLE_CLIENT_ID}
              />
            </div>

            {!smUp && (
              <Typography variant="body2" align="center" sx={{ mt: 3 }}>
                Don’t have an account?{' '}
                <Link variant="subtitle2" component={RouterLink} to="www.ismycustomermoving.com/#pricing">
                  Get started
                </Link>
              </Typography>
            )}
          </ContentStyle>
        </Container>
      </RootStyle>
    </Page>
  );
}
