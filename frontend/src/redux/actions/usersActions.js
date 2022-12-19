import axios from 'axios';
import { createSlice } from '@reduxjs/toolkit';
// import { LIST_REQUEST, LIST_SUCCESS, LIST_FAIL, STATUS_REQUEST, STATUS_SUCCESS, STATUS_FAIL, ADDUSER_REQUEST, ADDUSER_SUCCESS, ADDUSER_FAIL, NOTE_REQUEST, NOTE_SUCCESS, NOTE_FAIL, DELETE_REQUEST, DELETE_SUCCESS, DELETE_FAIL, COMPANY_FAIL, COMPANY_REQUEST, COMPANY_SUCCESS } from '../types/users';
import { DOMAIN } from '../constants';
import { logout } from './authActions';


export const userSlice = createSlice({
  name: "user",
  initialState: {
    clientsInfo: {
      loading: false,
      error: null,
      USERLIST: [],
    }
  },
  reducers: {
    // -----------------  USERS  -----------------
    users: (state, action) => {
      state.clientsInfo.USERLIST = action.payload;
      state.clientsInfo.loading = false;
      state.clientsInfo.error = null;
    },
    usersError: (state, action) => {
      state.clientsInfo.error = action.payload;
      state.clientsInfo.loading = false;
      state.clientsInfo.USERLIST = [];
    },
    usersLoading: (state) => {
      state.clientsInfo.loading = true;
      state.clientsInfo.USERLIST = [];
    },

  },
});

export const { users, usersLoading, usersError } = userSlice.actions;
export const selectUsers = (state) => state.user.clientsInfo;
export default userSlice.reducer;

export const usersAsync = () => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${userInfo.access}`,
      },
    };
    dispatch(usersLoading());
    const { data } = await axios.get(`${DOMAIN}/api/v1/accounts/clients/${userInfo.company.id}`, config);
    dispatch(users(data));
  } catch (error) {
    localStorage.removeItem('userInfo');
    dispatch(usersError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
    dispatch(logout());
  }
};

export const usersEditAsync = (id, status) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${userInfo.access}`,
      },
    };
    dispatch(usersLoading());
    const { data } = await axios.put(`${DOMAIN}/api/v1/accounts/clients/${id}/`, { status }, config);
    dispatch(users(data));
  } catch (error) {
    localStorage.removeItem('userInfo');
    dispatch(usersError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
    dispatch(logout());
  }
};

export const deleteClientAsync = (ids) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const {id: company} = userInfo.company;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${userInfo.access}`,
      },
    };
    dispatch(usersLoading());
    const { data } = await axios.delete(`${DOMAIN}/api/v1/accounts/updateclient/${company}/`, { data: {'clients': ids}}, config);
    dispatch(users(data));
  } catch (error) {
    dispatch(usersError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const updateClientAsync = (id, contacted, note) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const {id: company} = userInfo.company;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${userInfo.access}`,
      },
    };
    dispatch(usersLoading());
    const { data } = await axios.put(`${DOMAIN}/api/v1/accounts/updateclient/${company}/`, { 'clients': id, contacted, note }, config);
    dispatch(users(data));
  } catch (error) {
    dispatch(usersError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const serviceTitanSync = () => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const {id: company} = userInfo.company;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${userInfo.access}`,
      },
    };
    // dispatch(usersLoading());
    const { data } = await axios.get(`${DOMAIN}/api/v1/accounts/servicetitan/${company}/`, config);
    // dispatch(users(data));
  } catch (error) {
    throw new Error(error);
    // dispatch(usersError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const createCompany = (company, email) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${userInfo.access}`,
      },
    };

    const { data } = await axios.post(
      `${DOMAIN}/api/v1/accounts/createCompany/`,
      { 'name': company, email},
      config
    );

  } catch (error) {
    throw new Error(error);
  }
};

export const manageUser = (email) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;

    const config = {
      headers: {
        'Content-type': 'application/json',
      },
    };

    await axios.post(
      `${DOMAIN}/api/v1/accounts/manageuser/${userInfo.company.id}/`,
      { email },
      config
    );

    } catch (error) {
    throw new Error(error);
  }
};

export const update = () => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${userInfo.access}`,
      },
    };
    await axios.get(`${DOMAIN}/api/v1/accounts/update/${userInfo.company}`, config);
  } catch (error) {
    throw new Error(error);
  }
};

// export const sendNewUserEmail = (email) => async (dispatch, getState) => {
//   try {
//     dispatch({
//       type: ADDUSER_REQUEST,
//     });
//     console.log(email)
//     const config = {
//       headers: {
//         'Content-type': 'application/json',
//       },
//     };

//     const {
//       userLogin: { userInfo },
//     } = getState();

//     const { data } = await axios.post(
//       `${DOMAIN}/api/v1/accounts/manageuser/${userInfo.company}/`,
//       { email },
//       config
//     );

//     dispatch({
//       type: ADDUSER_SUCCESS,
//       payload: data,
//     });

//     dispatch({
//       type: ADDUSER_SUCCESS,
//       payload: data,
//     });
    
//   } catch (error) {
//     dispatch({
//       type: ADDUSER_FAIL,
//       payload: error.response && error.response.data.detail ? error.response.data.detail : error.message,
//     });
//   }
// };

// export const users = () => async (dispatch, getState) => {
//   try {
//     dispatch({
//       type: LIST_REQUEST,
//     });

//     const {
//       userLogin: { userInfo },
//     } = getState();

//     const config = {
//       headers: {
//         'Content-type': 'application/json',
//         Authorization: `Bearer ${userInfo.access}`,
//       },
//     };

//     const { data } = await axios.get(`${DOMAIN}/api/v1/accounts/clients/${userInfo.company.id}`, config);

//     dispatch({
//       type: LIST_SUCCESS,
//       payload: data,
//     });
//   } catch (error) {
//     localStorage.removeItem('userInfo');
//     dispatch({
//       type: LIST_FAIL,
//       payload: error.response && error.response.data.detail ? error.response.data.detail : error.message,
//     });
//   }
// };

// export const update = () => async (dispatch, getState) => {
//   try {
//     dispatch({
//       type: STATUS_REQUEST,
//     });

//     const {
//       userLogin: { userInfo },
//     } = getState();

//     const config = {
//       headers: {
//         'Content-type': 'application/json',
//         Authorization: `Bearer ${userInfo.access}`,
//       },
//     };

//     const { data } = await axios.get(`${DOMAIN}/api/v1/accounts/update/${userInfo.company}`, config);
//     dispatch({
//       type: STATUS_SUCCESS,
//       payload: data,
//     });
//   } catch (error) {
//     dispatch({
//       type: STATUS_FAIL,
//       payload: error.response && error.response.data.detail ? error.response.data.detail : error.message,
//     });
//   }
// };

// export const updateNote = (note, id) => async (dispatch, getState) => {
//   try {
//     dispatch({
//       type: NOTE_REQUEST,
//     });

//     const {
//       userLogin: { userInfo },
//     } = getState();

//     const config = {
//       headers: {
//         'Content-type': 'application/json',
//         Authorization: `Bearer ${userInfo.access}`,
//       },
//     };

//     const { data } = await axios.post(
//       `${DOMAIN}/api/v1/accounts/updatenote/${userInfo.company}/`,
//       { note, id},
//       config
//     );
//     dispatch({
//       type: NOTE_SUCCESS,
//       payload: data,
//     });
//   } catch (error) {
//     dispatch({
//       type: NOTE_FAIL,
//       payload: error.response && error.response.data.detail ? error.response.data.detail : error.message,
//     });
//   }
//   // users();
// };

// export const contact = (id) => async (dispatch, getState) => {
//   try {
//     dispatch({
//       type: NOTE_REQUEST,
//     });

//     const {
//       userLogin: { userInfo },
//     } = getState();

//     const config = {
//       headers: {
//         'Content-type': 'application/json',
//         Authorization: `Bearer ${userInfo.access}`,
//       },
//     };
//     const { data } = await axios.post(
//       `${DOMAIN}/api/v1/accounts/contacted/${userInfo.company}/`,
//       {id},
//       config
//     );
//     dispatch({
//       type: NOTE_SUCCESS,
//       payload: data,
//     });
//   } catch (error) {
//     dispatch({
//       type: NOTE_FAIL,
//       payload: error.response && error.response.data.detail ? error.response.data.detail : error.message,
//     });
//   }
// };

// export const deleteClient = (selectedClients) => async (dispatch, getState) => {
//   try {
//     dispatch({
//       type: DELETE_REQUEST,
//     });

//     const {
//       userLogin: { userInfo },
//     } = getState();

//     const config = {
//       headers: {
//         'Content-type': 'application/json',
//         Authorization: `Bearer ${userInfo.access}`,
//       },
//     };

//     const { data } = await axios.post(
//       `${DOMAIN}/api/v1/accounts/deleteClient/${userInfo.company}/`,
//       selectedClients,
//       config
//     );
//     dispatch({
//       type: DELETE_SUCCESS,
//       payload: data,
//     });
//   } catch (error) {
//     dispatch({
//       type: DELETE_FAIL,
//       payload: error.response && error.response.data.detail ? error.response.data.detail : error.message,
//     });
//   }
// };

// export const createCompany = (company, email) => async (dispatch, getState) => {
//   try {
//     dispatch({
//       type: NOTE_REQUEST,
//     });

//     const {
//       userLogin: { userInfo },
//     } = getState();

//     const config = {
//       headers: {
//         'Content-type': 'application/json',
//         Authorization: `Bearer ${userInfo.access}`,
//       },
//     };

//     const { data } = await axios.post(
//       `${DOMAIN}/api/v1/accounts/createCompany/`,
//       { 'name': company, email},
//       config
//     );
//     dispatch({
//       type: NOTE_SUCCESS,
//       payload: data,
//     });
//   } catch (error) {
//     dispatch({
//       type: NOTE_FAIL,
//       payload: error.response && error.response.data.detail ? error.response.data.detail : error.message,
//     });
//   }
// };

// export const updateCompany = (email, phone, tenantID) => async (dispatch, getState) => {
//   try {
//     dispatch({
//       type: COMPANY_REQUEST,
//     });

//     const {
//       userLogin: { userInfo },
//     } = getState();

//     const config = {
//       headers: {
//         'Content-type': 'application/json',
//         Authorization: `Bearer ${userInfo.access}`,
//       },
//     };

//     const { data } = await axios.put(
//       `${DOMAIN}/api/v1/accounts/company/`,
//       { 'company': userInfo.company.id, email, phone, tenantID, 'user': userInfo.id},
//       config
//     );
//     dispatch({
//       type: COMPANY_SUCCESS,
//       payload: data,
//     });

//     localStorage.setItem('userInfo', JSON.stringify(data));
//     console.log(localStorage.getItem('userInfo'));
    

//   } catch (error) {
//     dispatch({
//       type: COMPANY_FAIL,
//       payload: error.response && error.response.data.detail ? error.response.data.detail : error.message,
//     });
//   }
