import axios from 'axios';
import { createSlice } from '@reduxjs/toolkit';
import { DOMAIN } from '../constants';
import { logout } from './authActions';


export const userSlice = createSlice({
  name: "user",
  initialState: {
    clientsInfo: {
      loading: false,
      error: null,
      CLIENTLIST: [],
      count: 0,
      forSale: {
        current: 0,
        total: 0,
      },
      recentlySold: {
        current: 0,
        total: 0,
      },
      highestPage: 0,
      deleted: 0,
      message: null,
      
    },
    usersInfo: {
      loading: false,
      error: null,
      USERLIST: [],
    },
    recentlySoldInfo: {
      loading: false,
      error: null,
      RECENTLYSOLDLIST: [],
      highestPage: 0,
      count: 0,
    },
    referralInfo: {
      loading: false,
      error: null,
      REFERRALLIST: [],
      highestPage: 0,
    }
  },
  reducers: {
    // -----------------  CLIENTS  -----------------
    clients: (state, action) => {
      state.clientsInfo.CLIENTLIST = action.payload.results.clients;
      state.clientsInfo.count = action.payload.count;
      state.clientsInfo.forSale.current = action.payload.results.forSale;
      state.clientsInfo.forSale.total = action.payload.results.forSaleAllTime;    
      state.clientsInfo.recentlySold.current = action.payload.results.recentlySold;
      state.clientsInfo.recentlySold.total = action.payload.results.recentlySoldAllTime;   
      state.clientsInfo.loading = false;
      state.clientsInfo.error = null;
      state.clientsInfo.done = false;
    },
    moreClients: (state, action) => {
      state.clientsInfo.CLIENTLIST = [...state.clientsInfo.CLIENTLIST, ...action.payload.results.clients];
      state.clientsInfo.loading = false;
      state.clientsInfo.error = null;
    },

    newPage: (state, action) => {
      state.clientsInfo.highestPage = action.payload;
    },
    clientsError: (state, action) => {
      state.clientsInfo.error = action.payload;
      state.clientsInfo.loading = false;
      state.clientsInfo.CLIENTLIST = [];
    },
    clientsLoading: (state) => {
      state.clientsInfo.loading = true;
    },
    clientsNotLoading: (state) => {
      state.clientsInfo.loading = false;
    },
    clientsNotAdded: (state, action) => {
      state.clientsInfo.deleted = action.payload;
    },
    clientsUpload: (state, action) => {
      state.clientsInfo.message = action.payload;
    },

    // -----------------  USERS  -----------------
    users: (state, action) => {
      state.usersInfo.USERLIST = action.payload;
      state.usersInfo.loading = false;
      state.usersInfo.error = null;
    },
    usersError: (state, action) => {
      state.usersInfo.error = action.payload;
      state.usersInfo.loading = false;
      state.usersInfo.USERLIST = [];
    },
    usersLoading: (state) => {
      state.usersInfo.loading = true;
      state.usersInfo.USERLIST = [];
    },

    // -----------------  RECENTLY SOLD  -----------------
    recentlySold: (state, action) => {
      state.recentlySoldInfo.RECENTLYSOLDLIST = action.payload.results;
      state.recentlySoldInfo.loading = false;
      state.recentlySoldInfo.error = null;
      state.recentlySoldInfo.count = action.payload.count;
    },
    recentlySoldError: (state, action) => {
      state.recentlySoldInfo.error = action.payload;
      state.recentlySoldInfo.loading = false;
      state.recentlySoldInfo.RECENTLYSOLDLIST = [];
    },
    recentlySoldLoading: (state) => {
      state.recentlySoldInfo.loading = true;
      state.recentlySoldInfo.RECENTLYSOLDLIST = [];
      state.recentlySoldInfo.highestPage = 0;
    },
    moreRecentlySold: (state, action) => {
      state.recentlySoldInfo.RECENTLYSOLDLIST = [...state.recentlySoldInfo.RECENTLYSOLDLIST, ...action.payload.results];
      state.recentlySoldInfo.loading = false;
      state.recentlySoldInfo.error = null;
      state.recentlySoldInfo.count = action.payload.count;
    },

    newRecentlySoldPage: (state, action) => {
      state.recentlySoldInfo.highestPage = action.payload;
    },

    // -----------------  REFERRALS  -----------------
    referrals: (state, action) => {
      state.referralInfo.REFERRALLIST = action.payload;
      state.referralInfo.loading = false;
      state.referralInfo.error = null;
    },
    referralsError: (state, action) => {
      state.referralInfo.error = action.payload;
      state.referralInfo.loading = false;
      state.referralInfo.REFERRALLIST = [];
    },
    referralsLoading: (state) => {
      state.referralInfo.loading = true;
    },
    moreReferrals: (state, action) => {
      state.referralInfo.REFERRALLIST = [...state.referralInfo.REFERRALLIST, ...action.payload];
      state.referralInfo.loading = false;
      state.referralInfo.error = null;
    },
    newReferralsPage: (state, action) => {
      state.referralInfo.highestPage = action.payload;
    },
    logoutClients: (state) => {
      state.clientsInfo.CLIENTLIST = [];
      state.clientsInfo.count = 0;
      state.clientsInfo.forSale.current = 0;
      state.clientsInfo.forSale.total = 0;
      state.clientsInfo.recentlySold.current = 0;
      state.clientsInfo.recentlySold.total = 0;
      state.clientsInfo.loading = false;
      state.clientsInfo.error = null;
      state.clientsInfo.done = false;
      state.clientsInfo.highestPage = 0;
      state.clientsInfo.deleted = 0;
      state.clientsInfo.message = null;
      state.usersInfo.USERLIST = [];
      state.usersInfo.loading = false;
      state.usersInfo.error = null;
      state.recentlySoldInfo.RECENTLYSOLDLIST = [];
      state.recentlySoldInfo.loading = false;
      state.recentlySoldInfo.error = null;
      state.recentlySoldInfo.count = 0;
      state.recentlySoldInfo.highestPage = 0;
      state.referralInfo.REFERRALLIST = [];
      state.referralInfo.loading = false;
      state.referralInfo.error = null;
      state.referralInfo.highestPage = 0;
    },


    // TODO
    sendNewUserEmail: (state) => {
      state.clientsInfo.loading = false;
      state.clientsInfo.error = null;
    },
    updateNote: (state) => {
      state.clientsInfo.loading = false;
      state.clientsInfo.error = null;
    }

  },
});

export const { clientsNotAdded, clients, moreClients, newPage, clientsUpload, clientsLoading, clientsNotLoading, clientsError,
   users, usersLoading, usersError,
   recentlySold, recentlySoldLoading, recentlySoldError, newRecentlySoldPage, moreRecentlySold,
   referrals, referralsLoading, referralsError, moreReferrals, newReferralsPage,
   logoutClients
  } = userSlice.actions;
export const selectClients = (state) => state.user.clientsInfo;
export const selectRecentlySold = (state) => state.user.recentlySoldInfo;
export const selectUsers = (state) => state.user.usersInfo;
export const selectReferrals = (state) => state.user.referralInfo;
export default userSlice.reducer;

export const usersAsync = (accessToken) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };
    dispatch(usersLoading());
    const { data } = await axios.get(`${DOMAIN}/api/v1/accounts/users/${userInfo.company.id}`, config);
    dispatch(users(data));
  } catch (error) {
    dispatch(usersError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
    dispatch(logout());
  }
};

export const deleteUserAsync = (accessToken, ids) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const {id: company} = userInfo.company;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };
    dispatch(usersLoading());
    const { data } = await axios.delete(`${DOMAIN}/api/v1/accounts/manageuser/${company}/`, { data: ids}, config);
    dispatch(users(data));
  } catch (error) {
    dispatch(usersError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const clientsAsync = (accessToken, page) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };
    if (page === 1) {
      dispatch(clientsLoading());
    }
    if (page > reduxStore.user.clientsInfo.highestPage || page === 1) {
      const { data } = await axios.get(`${DOMAIN}/api/v1/data/clients/${userInfo.id}?page=${page}`, config);
      if (data.results.clients.length > 0) {
        dispatch(newPage(page));      }
      if (page === 1) {
        dispatch(clients(data));        
      } else {
        dispatch(moreClients(data));
      }
    } else {
      dispatch(clientsNotLoading());
    }
  } catch (error) {
    localStorage.removeItem('userInfo');
    dispatch(clientsError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
    dispatch(logout());
  }
};

export const deleteClientAsync = (accessToken, ids) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };
    dispatch(clientsLoading());
    const chunkSize = 1000;
    let i = 0;
    for (i; i < ids.length; i += chunkSize) {
      const chunk = ids.slice(i, i + chunkSize);
      await axios.delete(`${DOMAIN}/api/v1/data/updateclient/`, { data: {'clients': chunk}}, config);
    }
    const chunk = ids.slice(i, i + chunkSize);
    await axios.delete(`${DOMAIN}/api/v1/data/updateclient/`, { data: {'clients': chunk}}, config);
    dispatch(clientsAsync(1));
  } catch (error) {
    dispatch(clientsError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const updateClientAsync = (accessToken, id, contacted, note) => async (dispatch) => {
  try {
    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };
    dispatch(clientsLoading());
    await axios.put(`${DOMAIN}/api/v1/data/updateclient/`, { 'clients': id, contacted, note }, config);
    dispatch(clientsAsync(1));
  } catch (error) {
    dispatch(clientsError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const uploadClientsUpdateAsync = (accessToken, id) => async (dispatch) => {
  try {
    const config = {
      headers: {
        'Content-type': 'application/json',
        'Authorization': `Bearer ${accessToken}`,
      },
    };
    const { data } = await axios.get(`${DOMAIN}/api/v1/data/upload/${id}/`, config);
    if (data.status === 'SUCCESS') {
      dispatch(clientsUpload(data.data))
      dispatch(clientsNotAdded(data.deleted))
      dispatch(clientsAsync(1));
    } else {
      setTimeout(() => {
        dispatch(uploadClientsUpdateAsync(accessToken, id));
      }, 1000);
    }
  } catch (error) {
    dispatch(clientsError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const uploadClientsAsync = (accessToken, customers) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const {id: company} = userInfo.company;
    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };

    dispatch(clientsLoading());
    const {data} = await axios.put(`${DOMAIN}/api/v1/data/upload/${company}/`, customers, config);
    dispatch(clientsUpload(data.data));
    dispatch(uploadClientsUpdateAsync(accessToken, data.task))
    } catch (error) {
    dispatch(clientsError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const filterClientsAsync = (accessToken, statusFilters, minPrice, maxPrice, minYear, maxYear, tagFilters, equipInstallDateMin, equipInstallDateMax) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };
    let filters = ""
    if (statusFilters.length > 0) {
      filters += `&status=${statusFilters.join(',')}`
    }
    if (minPrice) {
      filters += `&min_price=${minPrice}`
    }
    if (maxPrice) {
      filters += `&max_price=${maxPrice}`
    }
    if (minYear) {
      filters += `&min_year=${minYear}`
    }
    if (maxYear) {
      filters += `&max_year=${maxYear}`
    }
    if (tagFilters.length > 0) {
      filters += `&tags=${tagFilters.join('&tags=')}`
    }
    if (equipInstallDateMin) {
      filters += `&equip_install_date_min=${equipInstallDateMin}`
    }
    if (equipInstallDateMax) {
      filters += `&equip_install_date_max=${equipInstallDateMax}`
    }
    const { data } = await axios.get(`${DOMAIN}/api/v1/data/clients/${userInfo.id}/?page=1${filters}`, config);
    dispatch(clients(data));
  } catch (error) {
    dispatch(clientsError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const serviceTitanUpdateAsync = (accessToken, id) => async (dispatch) => {
  try {
    const config = {
      headers: {
        'Content-type': 'application/json',
        'Authorization': `Bearer ${accessToken}`,
      },
    };
    const { data } = await axios.get(`${DOMAIN}/api/v1/data/servicetitan/${id}/`, config);
    if (data.status === 'SUCCESS') {
      dispatch(clientsNotAdded(data.deleted))
      dispatch(clientsUpload(data.data))
      dispatch(clientsAsync(1));
    } else {     
      setTimeout(() => {
        dispatch(serviceTitanUpdateAsync(accessToken, id));
      }, 1000);
      
    }
  } catch (error) {
    dispatch(clientsError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};
        

export const serviceTitanSync = (accessToken) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const {id: company} = userInfo.company;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };
    dispatch(clientsLoading());
    const { data } = await axios.put(`${DOMAIN}/api/v1/data/servicetitan/${company}/`, config);
    dispatch(serviceTitanUpdateAsync(accessToken, data.task))
    
  } catch (error) {
    throw new Error(error);
    // dispatch(usersError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const salesForceSync = (accessToken) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const {id: company} = userInfo.company;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };
    // dispatch(clientsLoading());
    await axios.put(`${DOMAIN}/api/v1/data/salesforce/${company}/`, config);
    dispatch(clientsAsync(1))
    
  } catch (error) {
    throw new Error(error);
    // dispatch(usersError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const createCompany = (accessToken, company, email) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };

    await axios.post(
      `${DOMAIN}/api/v1/accounts/createCompany/`,
      { 'name': company, email},
      config
    );

  } catch (error) {
    throw new Error(error);
  }
};

export const manageUser = (accessToken, email) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };

     dispatch(usersLoading());
    const {data} = await axios.post(
      `${DOMAIN}/api/v1/accounts/manageuser/${userInfo.company.id}/`,
      { email },
      config
    );
    dispatch(users(data));

    } catch (error) {
      dispatch(usersError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const makeAdminAsync = (accessToken, userId) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;

    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };

    dispatch(usersLoading());
    const {data} = await axios.post(
      `${DOMAIN}/api/v1/accounts/manageuser/${userId}/`,
      config
    );
    dispatch(users(data));

    } catch (error) {
      dispatch(usersError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const update = (accessToken) => async (getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };
    await axios.get(`${DOMAIN}/api/v1/data/update/${userInfo.company.id}`, config);
  } catch (error) {
    throw new Error(error);
  }
};

export const recentlySoldAsync = (accessToken, page) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };
    if (page === 1) {
      dispatch(recentlySoldLoading());
    }
    if (page > reduxStore.user.recentlySoldInfo.highestPage) {
      const { data } = await axios.get(`${DOMAIN}/api/v1/data/recentlysold/${userInfo.company.id}?page=${page}`, config);
      if (data.results.length > 0) {
        dispatch(newRecentlySoldPage(page));
      }
      if (page === 1) {
        dispatch(recentlySold(data));        
      } else {
        dispatch(moreRecentlySold(data));
      }
    }
  } catch (error) {
    dispatch(recentlySoldError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
}

export const filterRecentlySoldAsync = (accessToken, minPrice, maxPrice, minYear, maxYear, minDaysAgo, maxDaysAgo, tagFilters) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };
    dispatch(recentlySoldLoading());
    let filters = ""
    if (minPrice) {
      filters += `&min_price=${minPrice}`
    }
    if (maxPrice) {
      filters += `&max_price=${maxPrice}`
    }
    if (minYear) {
      filters += `&min_year=${minYear}`
    }
    if (maxYear) {
      filters += `&max_year=${maxYear}`
    }
    if (minDaysAgo) {
      filters += `&min_days_ago=${minDaysAgo}`
    }
    if (maxDaysAgo) {
      filters += `&max_days_ago=${maxDaysAgo}`
    }
    const { data } = await axios.get(`${DOMAIN}/api/v1/data/recentlysold/${userInfo.company.id}/?page=1${filters}`, config);
    dispatch(recentlySold(data));   
  } catch (error) {
    dispatch(recentlySoldError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
};

export const makeReferralAsync = (accessToken, id, area) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };
    await axios.post(`${DOMAIN}/api/v1/accounts/referrals/${userInfo.company.id}/`, {id, area}, config);
  } catch (error) {
    throw new Error(error);
  }
}

export const referralsAsync = (accessToken, page) => async (dispatch, getState) => {
  try {
    const reduxStore = getState();
    const {userInfo} = reduxStore.auth.userInfo;
    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
    };
    if (page === 1) {
      dispatch(referralsLoading());
    }
    if (page > reduxStore.user.referralInfo.highestPage) {
      const { data } = await axios.get(`${DOMAIN}/api/v1/accounts/referrals/${userInfo.company.id}?page=${page}`, config);
      if (data.results.length > 0) {
        dispatch(newReferralsPage(page));
      }
      if (page === 1) {
        dispatch(referrals(data));        
      } else {
        dispatch(moreReferrals(data));
      }
    }
  } catch (error) {
    console.log("error", error)
    dispatch(referralsError(error.response && error.response.data.detail ? error.response.data.detail : error.message));
  }
}