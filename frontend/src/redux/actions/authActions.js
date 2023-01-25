import axios from 'axios';
import { createSlice } from '@reduxjs/toolkit';

import { DOMAIN } from '../constants';

export const authSlice = createSlice({
  name: "auth",
  initialState: {
    userInfo: {
      userInfo: localStorage.getItem('userInfo') ? JSON.parse(localStorage.getItem('userInfo')) : null,
      loading: false,
      error: null,
      twoFA: localStorage.getItem('twoFA') ? JSON.parse(localStorage.getItem('twoFA')) : false,
    },
    registerInfo: {
      loading: false,
      error: null,
    },
  },
  reducers: {
    // -----------------  AUTH  -----------------
    login: (state, action) => {
      state.userInfo.userInfo = action.payload;
      state.userInfo.error = null;
      state.userInfo.loading = false;
    },
    validate: (state, action) => {
      state.userInfo.userInfo = action.payload;
      state.userInfo.error = null;
      state.userInfo.loading = false;
      state.userInfo.twoFA = true;
    },
    loginError: (state, action) => {
      state.userInfo.error = action.payload;
      state.userInfo.loading = false;
    },
    loginLoading: (state) => {
      state.userInfo.loading = true;
    },
    register: (state, action) => {
      state.userInfo.userInfo = action.payload;
      state.registerInfo.error = null;
      state.registerInfo.loading = false;
    },
    registerError: (state, action) => {
      state.registerInfo.error = action.payload;
      state.registerInfo.loading = false;
    },
    registerLoading: (state) => {
      state.registerInfo.loading = true;
    },
    reset: (state) => {
      state.registerInfo.error = null;
      state.registerInfo.loading = false;
    },
    logoutUser: (state) => {
      state.userInfo = {
        userInfo: null,
        loading: false,
        error: null,
      };
    },
    company: (state, action) => {
      state.userInfo.userInfo = action.payload;
      state.userInfo.loading = false;
      state.userInfo.error = null;
    },
    companyError: (state, action) => {
      state.userInfo.error = action.payload;
      state.userInfo.loading = false;
    },
    companyLoading: (state) => {
      state.userInfo.loading = true;
    },

  }
});

export const loginAsync = (email, password) => async (dispatch) => {
  try {
    dispatch(loginLoading()); 
    const config = {
      headers: {
        'Content-type': 'application/json',
      },
    };
    const { data } = await axios.post(`${DOMAIN}/api/v1/accounts/login/`, { email, password }, config);
    dispatch(login(data));
    localStorage.setItem('userInfo', JSON.stringify(data));
    
  } catch (error) {
    dispatch(loginError(error.response && error.response.data.non_field_errors[0] ? error.response.data.non_field_errors[0] : error.message,));
  }
};

export const editUserAsync = (email, firstName, lastName, serviceTitan) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${userInfo.access}`,
      },
    };
    dispatch(loginLoading());
    const { data } = await axios.put(`${DOMAIN}/api/v1/accounts/manageuser/${userInfo.id}/`, { email, firstName, lastName, serviceTitan }, config);
    dispatch(login(data));
    localStorage.setItem('userInfo', JSON.stringify(data));
  } catch (error) {
    dispatch(loginError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const registerAsync = (company, accessToken, firstName, lastName, email, password) => async (dispatch) => {
  try {
    dispatch(registerLoading());
    const config = {
      headers: {
        'Content-type': 'application/json',
      },
    };
    const { data } = await axios.post(`${DOMAIN}/api/v1/accounts/register/`, { company, accessToken, firstName, lastName, email, password }, config);    
    dispatch(register(data));
    localStorage.setItem('userInfo', JSON.stringify(data));
  } catch (err) {
    dispatch(registerError(err.response && err.response.data.detail ? err.response.data.detail : err.message,));
  }
};

export const companyAsync = (email, phone, tenantID, clientID, clientSecret, forSaleTag, forRentTag, soldTag) => async (dispatch, getState) => {
  try {
    dispatch(companyLoading());

    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;

    const config = {
      headers: {
        'Content-type': 'application/json',
      },
    };
    const { data } = await axios.put(
      `${DOMAIN}/api/v1/accounts/company/`,
      { 'company': userInfo.company.id, email, phone, tenantID, clientID, clientSecret, forSaleTag, forRentTag, soldTag, 'user': userInfo.id},
      config
    );
    dispatch(company(data));
    localStorage.setItem('userInfo', JSON.stringify(data));
  } catch (err) {
    console.log(err)
    dispatch(companyError(err.response && err.response.data.detail ? err.response.data.detail : err.message,));
  }
};

// export const resetPasswordAsync = (oldPassword, password) => async (dispatch) => {
// };

export const addUserAsync = (firstName, lastName, email, password, token) => async (dispatch) => {
  try {
    dispatch(registerLoading());
    const config = {
      headers: {
        'Content-type': 'application/json',
      },
    };

    const { data } = await axios.post(
      `${DOMAIN}/api/v1/accounts/manageuser/${token}/`,
      { firstName, lastName, email, password },
      config
    );
    dispatch(register(data));
    localStorage.setItem('userInfo', JSON.stringify(data));
  } catch (err) {
    dispatch(registerError(err.response && err.response.data.detail ? err.response.data.detail : err.message,));
  }
};

export const resetAsync = (email) => async (dispatch) => {
  try {
    const config = {
      headers: {
        'Content-type': 'application/json',
      },
    };
    dispatch(registerLoading());
    await axios.post(`${DOMAIN}/api/v1/accounts/password_reset/`, { email }, config);
    dispatch(reset());
  } catch (err) {
    dispatch(registerError(err.response && err.response.data.detail ? err.response.data.detail : err.message,));
  }
};

export const submitNewPassAsync = (password, token) => async (dispatch) => {
  try {
    const config = {
      headers: {
        'Content-type': 'application/json',
      },
    };

    dispatch(registerLoading());
    await axios.post(
      `${DOMAIN}/api/v1/accounts/password_reset/confirm/`,
      { 
        token,
        password
      },
      config
    );
  } catch (err) {
    dispatch(registerError(err.response && err.response.data.detail ? err.response.data.detail : err.message,));
  }
};

export const logout = () => (dispatch) => {
  console.log("logging out")
  localStorage.removeItem('userInfo');
  localStorage.removeItem('twoFA');
  dispatch(logoutUser());
};

export const generateQrCodeAsync = () => async (dispatch, getState) => {
  try {
    const config = {
      headers: {
        'Content-type': 'application/json',
      },
    };
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const {id} = userInfo;
    dispatch(loginLoading());
    const { data } = await axios.post(`${DOMAIN}/api/v1/accounts/otp/generate/`, { id }, config);
    dispatch(login(data));
    localStorage.setItem('userInfo', JSON.stringify(data));
  } catch (error) {
    dispatch(loginError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const verifyOtp = (otp) => async (dispatch, getState) => {
  try {
    const config = {
      headers: {
        'Content-type': 'application/json',
      },
    };
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const {id} = userInfo;
    dispatch(loginLoading());
    const { data } = await axios.post(`${DOMAIN}/api/v1/accounts/otp/verify/`, { id, otp }, config);
    dispatch(login(data));
    localStorage.setItem('userInfo', JSON.stringify(data));
    localStorage.setItem('twoFA', JSON.stringify(true));
  } catch (error) {
    dispatch(loginError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const validateOtp = (otp) => async (dispatch, getState) => {
  try {
    const config = {
      headers: {
        'Content-type': 'application/json',
      },
    };
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const {id} = userInfo;
    dispatch(loginLoading());
    const { data } = await axios.post(`${DOMAIN}/api/v1/accounts/otp/validate/`, { id, otp }, config);
    dispatch(validate(data));
    localStorage.setItem('userInfo', JSON.stringify(data));
    localStorage.setItem('twoFA', JSON.stringify(true));
  } catch (error) {
    dispatch(loginError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const disableTwoFactorAuth = () => async (dispatch, getState) => {
  try {
    const config = {
      headers: {
        'Content-type': 'application/json',
      },
    };
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const {id} = userInfo;
    dispatch(loginLoading());
    const { data } = await axios.post(`${DOMAIN}/api/v1/accounts/otp/disable/`, { id }, config);
    dispatch(login(data));
    localStorage.setItem('userInfo', JSON.stringify(data));
  } catch (error) {
    dispatch(loginError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const { login, validate, loginError, loginLoading, register, registerError, registerLoading, logoutUser, company, companyError, companyLoading, reset } = authSlice.actions;
export const showLoginInfo = (state) => state.auth.userInfo;
export const showRegisterInfo = (state) => state.auth.registerInfo;
export default authSlice.reducer;