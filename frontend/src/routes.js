import { Navigate, useRoutes } from 'react-router-dom';
// layouts
import DashboardLayout from './layouts/dashboard';
import LogoOnlyLayout from './layouts/LogoOnlyLayout';
//
import CustomerData from './pages/CustomerData';
import Login from './pages/Login';
import NotFound from './pages/Page404';
import Register from './pages/Register';
import AddUser from './pages/AddUser';

import ProfileSettings from './pages/ProfileSettings';

// ----------------------------------------------------------------------

export default function Router() {
  return useRoutes([
    {
      path: '/dashboard',
      element: <DashboardLayout />,
      children: [
        { path: '', element: <Navigate to="/dashboard/customers" /> },
        { path: 'settings', element: <ProfileSettings /> },
        { path: 'customers', element: <CustomerData /> },
        { path: 'adduser', element: <AddUser />},
      ],
    },
    {
      path: '/',
      element: <LogoOnlyLayout />,
      children: [
        { path: '/', element: <Navigate to="/dashboard/customers" /> },
        { path: 'login', element: <Login /> },
        { path: 'register', element: <Register /> },
        { path: '404', element: <NotFound /> },
        { path: '*', element: <Navigate to="/404" /> },
        
      ],
    },
    { path: '*', element: <Navigate to="/404" replace /> },
  ]);
}
