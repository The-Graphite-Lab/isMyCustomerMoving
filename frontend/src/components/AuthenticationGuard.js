import { withAuthenticationRequired } from '@auth0/auth0-react';
import React from 'react';
import { PageLoader } from './PageLoader';
import PropTypes from 'prop-types';

export const AuthenticationGuard = ({ component }) => {
  const Component = withAuthenticationRequired(component, {
    onRedirecting: () => (
      <div className="page-layout">
        <PageLoader />
      </div>
    ),
  });

  return <Component />;
};

AuthenticationGuard.propTypes = {
  component: PropTypes.elementType.isRequired,
};
