import * as Yup from 'yup';
import { useState, useEffect } from 'react';
import { useFormik, Form, FormikProvider } from 'formik';
import { useNavigate } from 'react-router-dom';
// material
import { Stack, TextField, IconButton, InputAdornment, Alert, AlertTitle } from '@mui/material';
import { LoadingButton } from '@mui/lab';
import { useDispatch, useSelector } from 'react-redux';
import { registerAsync, showLoginInfo, showRegisterInfo } from '../../../redux/actions/authActions';
// component
import Iconify from '../../../components/Iconify';

// ----------------------------------------------------------------------

export default function RegisterForm() {
  const dispatch = useDispatch();
  const navigate = useNavigate();

  const userLogin = useSelector(showLoginInfo);
  const { error: loginError, loading: loginLoading, userInfo } = userLogin;

  const userRegister = useSelector(showRegisterInfo);
  const { error: registerError, loading: registerLoading } = userRegister;


  const [showPassword, setShowPassword] = useState(false);

  const RegisterSchema = Yup.object().shape({
    firstName: Yup.string().min(2, 'Too Short!').max(50, 'Too Long!').required('First name required'),
    lastName: Yup.string().min(2, 'Too Short!').max(50, 'Too Long!').required('Last name required'),
    company: Yup.string().min(2, 'Too Short!').max(50, 'Too Long!').required('Company required'),
    accessToken: Yup.string().min(2, 'Too Short!').max(50, 'Too Long!').required('Access token required'),
    email: Yup.string().email('Email must be a valid email address').required('Email is required'),
    password: Yup.string().required('Password is required'),
  });

  const formik = useFormik({
    initialValues: {
      company: '',
      accessToken: '',
      firstName: '',
      lastName: '',
      email: '',
      password: '',
    },
    validationSchema: RegisterSchema,
    onSubmit: () => {
      dispatch(registerAsync(values.company, values.accessToken, values.firstName, values.lastName, values.email, values.password));
    },
  });

  const { errors, touched, values, handleSubmit, isSubmitting, getFieldProps } = formik;
  useEffect(() => {
    if (userInfo) {
      navigate('/dashboard/customers', { replace: true });
    }
  }, [navigate, userInfo]);
  return (
    <FormikProvider value={formik}>
      <Form autoComplete="off" noValidate onSubmit={handleSubmit}>
        <Stack spacing={3}>
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              fullWidth
              label="Company Name"
              {...getFieldProps('company')}
              error={Boolean(touched.company && errors.company)}
              helperText={touched.company && errors.company}
            />
            <TextField
              fullWidth
              label="Access Token"
              {...getFieldProps('accessToken')}
              error={Boolean(touched.accessToken && errors.accessToken)}
              helperText={touched.accessToken && errors.accessToken}
            />
          </Stack>
          
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>

            <TextField
              fullWidth
              label="First name"
              {...getFieldProps('firstName')}
              error={Boolean(touched.firstName && errors.firstName)}
              helperText={touched.firstName && errors.firstName}
            />

            <TextField
              fullWidth
              label="Last name"
              {...getFieldProps('lastName')}
              error={Boolean(touched.lastName && errors.lastName)}
              helperText={touched.lastName && errors.lastName}
            />
          </Stack>

          <TextField
            fullWidth
            autoComplete="username"
            type="email"
            label="Email address"
            {...getFieldProps('email')}
            error={Boolean(touched.email && errors.email)}
            helperText={touched.email && errors.email}
          />

          <TextField
            fullWidth
            autoComplete="current-password"
            type={showPassword ? 'text' : 'password'}
            label="Password"
            {...getFieldProps('password')}
            InputProps={{
              endAdornment: (
                <InputAdornment position="end">
                  <IconButton edge="end" onClick={() => setShowPassword((prev) => !prev)}>
                    <Iconify icon={showPassword ? 'eva:eye-fill' : 'eva:eye-off-fill'} />
                  </IconButton>
                </InputAdornment>
              ),
            }}
            error={Boolean(touched.password && errors.password)}
            helperText={touched.password && errors.password}
          />

          {registerError ? (
            <Alert severity="error">
              <AlertTitle>Register Error</AlertTitle>
              {registerError}
            </Alert>
          ) : null}

          <LoadingButton
            fullWidth
            size="large"
            type="submit"
            variant="contained"
            loading={registerLoading ? isSubmitting : null}
          >
            Register
          </LoadingButton>
        </Stack>
      </Form>
    </FormikProvider>
  );
}
