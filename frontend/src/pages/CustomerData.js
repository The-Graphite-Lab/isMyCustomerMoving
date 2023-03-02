/* eslint-disable camelcase */
import _, { filter} from 'lodash';
import axios from 'axios';
import { sentenceCase } from 'change-case';
import React, { useState, useEffect } from 'react';
import { Link as RouterLink, useNavigate } from 'react-router-dom';
// material
import {
  IconButton,
  Box,
  Card,
  Table,
  Stack,
  Button,
  Checkbox,
  LinearProgress,
  TableRow,
  TableBody,
  TableCell,
  Container,
  Typography,
  TableContainer,
  TablePagination,
  CircularProgress,
} from '@mui/material';
import { useSelector, useDispatch } from 'react-redux';
import { DOMAIN } from '../redux/constants';

// components
import ReferralModal from '../components/ReferralModal';
import NoteModal from '../components/NoteModal';
import Page from '../components/Page';
import Label from '../components/Label';
import FileUploader from '../components/FileUploader';
import Scrollbar from '../components/Scrollbar';
import Iconify from '../components/Iconify';
import SearchNotFound from '../components/SearchNotFound';
import CounterCard from '../components/CounterCard';
import ClientEventTable from '../components/ClientEventTable';
import { ClientListHead, ClientListToolbar } from '../sections/@dashboard/client';

import ClientsListCall from '../redux/calls/ClientsListCall';
import { selectClients, update, updateClientAsync, serviceTitanSync, salesForceSync, clientsAsync } from '../redux/actions/usersActions';
import { logout, showLoginInfo } from '../redux/actions/authActions';

// ----------------------------------------------------------------------


// ----------------------------------------------------------------------
// change this to sort by status
function descendingComparator(a, b, orderBy) {
  if (b[orderBy] < a[orderBy]) {
    return -1;
  }
  if (b[orderBy] > a[orderBy]) {
    return 1;
  }
  return 0;
}

export function getComparator(order, orderBy) {
  return order === 'desc'
    ? (a, b) => descendingComparator(a, b, orderBy)
    : (a, b) => -descendingComparator(a, b, orderBy);
}

export function applySortFilter(array, comparator, query, userInfo) {
  let stabilizedThis = array;  
  if (userInfo === 'admin') {
    stabilizedThis = array.map((el, index) => [el, index]);
  } else {
    stabilizedThis = array.filter(el => el.status !== 'No Change').map((el, index) => [el, index]);
  }
  stabilizedThis.sort((a, b) => {
    const order = comparator(a[0], b[0]);
    if (order !== 0) return order;
    return a[1] - b[1];
  });
  if (query) {
    return filter(array, (_user) => _.some(_user, val=>val && val.toString().toLowerCase().includes(query.toLowerCase())));;
  }
  return stabilizedThis.map((el) => el[0]);
}

export default function CustomerData() {
  const dispatch = useDispatch();
  const navigate = useNavigate();  

  const userLogin = useSelector(showLoginInfo);
  const { userInfo, twoFA } = userLogin;
  useEffect(() => {
    if (!userInfo) {
      dispatch(logout());
      navigate('/login', { replace: true });
      window.location.reload(false);
    } else if (userInfo.otp_enabled && twoFA === false) {
      navigate('/login', { replace: true });
    }

  }, [userInfo, dispatch, navigate]);
  
  const [TABLE_HEAD, setTABLE_HEAD] = useState([{ id: 'name', label: 'Name', alignRight: false },
        { id: 'address', label: 'Address', alignRight: false },
        { id: 'city', label: 'City', alignRight: false },
        { id: 'state', label: 'State', alignRight: false },
        { id: 'zipCode', label: 'Zip Code', alignRight: false },
        { id: 'status', label: 'Status', alignRight: false },
        { id: 'contacted', label: 'Contacted', alignRight: false },
        { id: 'note', label: 'Note', alignRight: false },
        { id: 'phone', label: 'Phone Number', alignRight: false }]);
  useEffect(() => {
    if (userInfo.company.franchise) {
      setTABLE_HEAD([
        { id: 'name', label: 'Name', alignRight: false },
        { id: 'address', label: 'Address', alignRight: false },
        { id: 'city', label: 'City', alignRight: false },
        { id: 'state', label: 'State', alignRight: false },
        { id: 'zipCode', label: 'Zip Code', alignRight: false },
        { id: 'status', label: 'Status', alignRight: false },
        { id: 'contacted', label: 'Contacted', alignRight: false },
        { id: 'note', label: 'Note', alignRight: false },
        { id: 'phone', label: 'Phone Number', alignRight: false },
        { id: 'referral', label: 'Refer', alignRight: false }
      ]);
    }
  }, [userInfo]);

  const listClient = useSelector(selectClients);
  const {loading, CLIENTLIST, forSale, recentlySold, count } = listClient;

  const [page, setPage] = useState(0);
  
  const [order, setOrder] = useState('asc');

  const [selected, setSelected] = useState([]);

  const [selectedClients, setSelectedClients] = useState([]);

  const [orderBy, setOrderBy] = useState('status');

  const [filterName, setFilterName] = useState('');

  const [rowsPerPage, setRowsPerPage] = useState(10);

  const [shownClients, setShownClients] = useState(0);

  const [expandedRow, setExpandedRow] = useState(null);

  const [csvLoading, setCsvLoading] = useState(false);

  const handleRowClick = (rowIndex) => {
    if (expandedRow === rowIndex) {
      setExpandedRow(null);
    } else {
      setExpandedRow(rowIndex);
    }
  };

  const handleRequestSort = (event, property) => {
    const isAsc = orderBy === property && order === 'asc';
    setOrder(isAsc ? 'desc' : 'asc');
    setOrderBy(property);
  };
  const handleSelectAllClick = (event) => {
    
    if (event.target.checked) {

      const newSelecteds = CLIENTLIST.slice((page * rowsPerPage), ((page+1) * rowsPerPage)).map((n) => n.address);
      setSelected(newSelecteds);
      
      const newSelectedClients = []
      for (let i=0; i < CLIENTLIST.length; i+=1) {
        newSelectedClients.push(CLIENTLIST[i].id)
      }
      setSelectedClients(newSelectedClients);
      return;
    }
    setSelected([]);
    setSelectedClients([]);
  };

  const handleClick = (event, address, id) => {
    const selectedIndex = selected.indexOf(address);
    let newSelected = [];
    let newSelectedClients = [];
    if (selectedIndex === -1) {
      newSelected = newSelected.concat(selected, address);
      newSelectedClients = newSelectedClients.concat(selectedClients, id);
    } else if (selectedIndex === 0) {
      newSelected = newSelected.concat(selected.slice(1));
      newSelectedClients = newSelectedClients.concat(selectedClients.slice(1));
    } else if (selectedIndex === selected.length - 1) {
      newSelected = newSelected.concat(selected.slice(0, -1));
      newSelectedClients = newSelectedClients.concat(selectedClients.slice(0, -1));
    } else if (selectedIndex > 0) {
      newSelected = newSelected.concat(selected.slice(0, selectedIndex), selected.slice(selectedIndex + 1));
      newSelectedClients = newSelectedClients.concat(selectedClients.slice(0, selectedIndex), selectedClients.slice(selectedIndex + 1));
    }
    setSelected(newSelected);
    setSelectedClients(newSelectedClients);

  };
  const handleChangePage = (event, newPage) => {
    // fetch new page if two away from needing to see new page
    if ((newPage+2) * rowsPerPage % 1000 === 0) {
      dispatch(clientsAsync(((newPage+2) * rowsPerPage / 1000)+1))
    }
    setPage(newPage);
  };
  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };
  const handleFilterByName = (event) => {
    setFilterName(event.target.value);
  };
  const updateContacted = (event, id, contacted) => {
    dispatch(updateClientAsync(id, contacted, ""));
  };
  const updateStatus = () => {
    dispatch(update());
  };
  const stSync = () => {
    dispatch(serviceTitanSync());
  };
  const sfSync = () => {
    dispatch(salesForceSync());
  };

  const exportCSV = async () => {
    if (CLIENTLIST.length === 0) { return }
    const config = {
      headers: {
        'Content-type': 'application/json',
        Authorization: `Bearer ${userInfo.access}`,
      },
    };
    setCsvLoading(true);
    const { data } = await axios.get(`${DOMAIN}/api/v1/accounts/allClients/${userInfo.id}`, config);
    let csvContent = 'data:text/csv;charset=utf-8,';
    csvContent += 'Name,Address,City,State,ZipCode,Status,Contacted,Note,Phone Number\r\n';
    data.forEach((n) => {
      csvContent += `${n.name}, ${n.address}, ${n.city}, ${n.state}, ${n.zipCode}, ${n.status}, ${n.contacted}, ${n.note}, ${n.phoneNumber}\r\n`
    });

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    // Create a download link for the CSV file
    const link = document.createElement('a');
    link.href = window.URL.createObjectURL(blob);
    const d1 = new Date().toLocaleDateString('en-US')
    const docName = `isMyCustomerMoving_${d1}`    

    link.setAttribute('download', `${docName}.csv`);
    document.body.appendChild(link); // add the link to body
    link.click();
    document.body.removeChild(link); // remove the link from body
    setCsvLoading(false);
  };
  const emptyRows = page > 0 ? Math.max(0, (1 + page) * rowsPerPage - CLIENTLIST.length) : 0;
  const filteredClients = userInfo ? applySortFilter(CLIENTLIST, getComparator(order, orderBy), filterName, userInfo.status) : [];
  
  useEffect(() => {
    if (filteredClients.length < CLIENTLIST.length) {
      setShownClients(filteredClients.length)
    } else {
      setShownClients(count)
    }
  }, [count, filteredClients])

  return (
    <Page title="User">
      <Container>
        {userInfo ? <ClientsListCall /> : null}
        {userInfo && (
          <>
            <Stack direction="row" alignItems="center" justifyContent="space-between" mb={5}>
              <Typography variant="h4" gutterBottom>
                Welcome {(userInfo.first_name).charAt(0).toUpperCase()+(userInfo.first_name).slice(1)} {(userInfo.last_name).charAt(0).toUpperCase()+(userInfo.last_name).slice(1)} 👋
              </Typography>              
              {/* {(userInfo.email === 'reid@gmail.com' || userInfo.email === 'jb@aquaclearws.com' || userInfo.email === 'reidelkins3@gmail.com') && (
                // <Button variant="contained" component={RouterLink} to="/dashboard/adduser" startIcon={<Iconify icon="eva:plus-fill" />}>
                <NewCompanyModal/>
              )} */}
            </Stack>
            <Stack direction="row" alignItems="center" justifyContent="space-around" mb={5} mx={10}>
              <Stack direction="column" alignItems="center" justifyContent="center">
                <CounterCard
                  start={0}
                  end={forSale.current}
                  title="For Sale"
                />
                <Typography variant="h6" gutterBottom mt={-3}> All Time: {forSale.total}</Typography>
              </Stack>

              <Stack direction="column" alignItems="center" justifyContent="center">
                <CounterCard
                  start={0}
                  end={recentlySold.current}
                  title="Recently Sold"
                />
                <Typography variant="h6" gutterBottom mt={-3}> All Time: {recentlySold.total}</Typography>
              </Stack>
            </Stack>
            <Card sx={{marginBottom:"3%"}}>
              <ClientListToolbar numSelected={selected.length} filterName={filterName} onFilterName={handleFilterByName} selectedClients={selectedClients} setSelected setSelectedClients />
              {loading ? (
                <Box sx={{ width: '100%' }}>
                  <LinearProgress />
                </Box>
              ) : null}

              <Scrollbar>
                <TableContainer sx={{ minWidth: 800 }}>
                  <Table>
                    <ClientListHead
                      order={order}
                      orderBy={orderBy}
                      headLabel={TABLE_HEAD}
                      rowCount={rowsPerPage}
                      numSelected={selected.length}
                      onRequestSort={handleRequestSort}
                      onSelectAllClick={handleSelectAllClick}
                      checkbox={1}
                    />
                    <TableBody>
                      {filteredClients.slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage).map((row) => {
                        const { id, name, address, city, state, zipCode, status, contacted, note, phoneNumber, clientUpdates_client: clientUpdates} = row;
                        const isItemSelected = selected.indexOf(address) !== -1;                        
                        
                        return (
                          <React.Fragment key={row.id}>
                            <TableRow
                              hover
                              key={id}
                              tabIndex={-1}
                              role="checkbox"
                              selected={isItemSelected}
                              aria-checked={isItemSelected}
                              onClick={() => handleRowClick(id)}
                            >
                              <TableCell padding="checkbox">
                                <Checkbox checked={isItemSelected} onChange={(event) => handleClick(event, address, id)} />
                              </TableCell>
                              <TableCell component="th" scope="row" padding="none">
                                <Stack direction="row" alignItems="center" spacing={2}>
                                  <Typography variant="subtitle2" noWrap>
                                    {name}
                                  </Typography>
                                </Stack>
                              </TableCell>
                              <TableCell align="left">{address}</TableCell>
                              <TableCell align="left">{city}</TableCell>
                              <TableCell align="left">{state}</TableCell>
                              <TableCell align="left">{zipCode}</TableCell>
                              <TableCell align="left">
                                {userInfo.email !== 'demo@demo.com' ? (
                                  <Label variant="ghost" color={(status === 'No Change' && 'warning') || (contacted === 'False' && 'error'  || 'success')}>
                                    {sentenceCase(status)}
                                  </Label>
                                ) : (
                                  <Label variant="ghost" color='warning'>
                                    Demo
                                  </Label>
                                )}
                                
                              </TableCell>
                              <TableCell>
                                {(() => {
                                  if (status !== 'No Change') {
                                    if (contacted) {
                                      return(
                                        <IconButton color="success" aria-label="View/Edit Note" component="label" onClick={(event)=>updateContacted(event, id, false)}>
                                          <Iconify icon="bi:check-lg" />
                                        </IconButton>
                                      )
                                    }
                                    return(
                                      <IconButton color="error" aria-label="View/Edit Note" component="label" onClick={(event)=>updateContacted(event, id, true)}>
                                        <Iconify icon="ps:check-box-empty" />
                                      </IconButton>
                                    )
                                  }
                                  return null;                                
                                })()}                          
                              </TableCell>
                              <TableCell>
                                <NoteModal 
                                  passedNote={note}
                                  id={id}
                                  name={name}
                                />
                              </TableCell>
                              <TableCell>
                                {/* make phone number look like (123) 456-7890 */}
                                {phoneNumber ? `(${phoneNumber.slice(0,3)}) ${phoneNumber.slice(3,6)}-${phoneNumber.slice(6,10)}`: "N/A"}
                              </TableCell>
                              {userInfo.company.franchise && (
                                <TableCell>
                                  {(() => {
                                    if (status !== 'No Change') {
                                      return(
                                        <ReferralModal id={id} alreadyReferred={false}/>
                                      )
                                    }
                                    return null;                                
                                  })()}                          
                                </TableCell>
                              )}
                              
                            </TableRow>                                                                            
                            {expandedRow === id && (
                              <TableRow style={{position:'relative', left:'10%'}}>
                                <TableCell/>
                                <TableCell colSpan={6}>
                                  <ClientEventTable clientUpdates={clientUpdates}/>
                                </TableCell>
                              </TableRow>
                            )}
                          </React.Fragment>
                        );
                      })}
                      {emptyRows > 0 && (
                        <TableRow style={{ height: 53 * emptyRows }}>
                          <TableCell colSpan={6} />
                        </TableRow>
                      )}
                    </TableBody>

                    {filteredClients.length === 0 && (
                      <TableBody>
                        <TableRow>
                          <TableCell align="center" colSpan={6} sx={{ py: 3 }}>
                            <SearchNotFound searchQuery={filterName} tipe="client"/>
                          </TableCell>
                        </TableRow>
                      </TableBody>
                    )}
                  </Table>
                </TableContainer>
              </Scrollbar>

              <TablePagination
                rowsPerPageOptions={[10, 50, 100]}
                component="div"
                count={shownClients}
                rowsPerPage={rowsPerPage}
                page={page}
                onPageChange={handleChangePage}
                onRowsPerPageChange={handleChangeRowsPerPage}
              />
            </Card>
            {loading ? (
              <Stack direction="row" alignItems="center" justifyContent="space-between" mb={5}>
                {((userInfo.first_name === 'reid' && userInfo.last_name === 'elkins') || (userInfo.first_name === 'Perspective' && userInfo.last_name === 'Customer')) && (
                  <Button variant="contained" >
                    <CircularProgress color="secondary"/>
                  </Button>
                )  }            

                {(userInfo.status === 'admin' && userInfo.finishedSTIntegration) && (
                  <Button variant="contained">
                    <CircularProgress color="secondary"/>
                  </Button>
                )}

                {(userInfo.status === 'admin') && (
                  <Button variant="contained">
                    <CircularProgress color="secondary"/>
                  </Button>
                )}
              </Stack>

            ):(
              <Stack direction="row" alignItems="center" justifyContent="space-between" mb={5}>
                {((userInfo.first_name === 'reid' && userInfo.last_name === 'elkins') || (userInfo.first_name === 'Perspective' && userInfo.last_name === 'Customer')) && (
                  <Button onClick={updateStatus} variant="contained" component={RouterLink} to="#" startIcon={<Iconify icon="eva:plus-fill" />}>
                    Update Status
                  </Button>
                )}        

                {userInfo.status === 'admin' && (                 
                  (
                    userInfo.company.crm === 'ServiceTitan' ? (
                      <Button onClick={stSync} variant="contained">
                        Sync With Service Titan
                      </Button>
                    )
                    :
                  (
                    userInfo.company.crm === 'Salesforce' && (
                      <Button onClick={sfSync} variant="contained">
                        Sync With Salesforce
                      </Button>
                    )
                  ))                  
                )}
                {csvLoading ? (
                  (userInfo.status === 'admin') && (
                    <Button variant="contained">
                      <CircularProgress color="secondary"/>
                    </Button>
                  )
                ):(
                  (userInfo.status === 'admin') && (
                    <Button onClick={exportCSV} variant="contained" component={RouterLink} to="#" startIcon={<Iconify icon="eva:plus-fill" />}>
                      Download To CSV
                    </Button>
                  )
                )}
                
              </Stack>
            )}
                                      
            { userInfo.status === 'admin' && (
                <FileUploader />
            )}
          </>
        )}
      </Container>
    </Page>
  );
}
