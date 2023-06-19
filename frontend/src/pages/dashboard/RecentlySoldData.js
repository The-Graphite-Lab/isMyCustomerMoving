/* eslint-disable camelcase */
import React, { useState, useEffect } from 'react';
import { Link as RouterLink } from 'react-router-dom';
// material
import {
  Button,
  Box,
  Card,
  Table,
  Stack,
  LinearProgress,
  TableRow,
  TableBody,
  TableCell,
  Link,
  Container,
  Typography,
  TableContainer,
  TablePagination,
  CircularProgress,
} from '@mui/material';

import { useSelector, useDispatch } from 'react-redux';

// components
import Page from '../../components/Page';
import Scrollbar from '../../components/Scrollbar';
import SearchNotFound from '../../components/SearchNotFound';
import { ClientListHead } from '../../sections/@dashboard/client';
import Iconify from '../../components/Iconify';

import { RecentlySoldListToolbar } from '../../sections/@dashboard/recentlySold';

import RecentlySoldListCall from '../../redux/calls/RecentlySoldListCall';
import { selectRecentlySold, recentlySoldAsync, getRecentlySoldCSV } from '../../redux/actions/usersActions';
import { showLoginInfo } from '../../redux/actions/authActions';
import { makeDate } from '../../utils/makeDate';

// ----------------------------------------------------------------------

const TABLE_HEAD = [
  { id: 'listed', label: 'Date Sold', alignRight: false },
  { id: 'address', label: 'Address', alignRight: false },
  { id: 'city', label: 'City', alignRight: false },
  { id: 'state', label: 'State', alignRight: false },
  { id: 'zipCode', label: 'Zip Code', alignRight: false },
  { id: 'price', label: 'Price', alignRight: false },
  { id: 'year_built', label: 'Year Built', alignRight: false },
];

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

export function applySortFilter(array, comparator) {
  const stabilizedThis = array.map((el, index) => [el, index]);

  stabilizedThis.sort((a, b) => {
    const order = comparator(a[0], b[0]);
    if (order !== 0) return order;
    return a[1] - b[1];
  });
  return stabilizedThis.map((el) => el[0]);
}

export default function RecentlySoldData() {
  const dispatch = useDispatch();

  const userLogin = useSelector(showLoginInfo);
  const { userInfo } = userLogin;

  const listRecentlySold = useSelector(selectRecentlySold);
  const { loading, RECENTLYSOLDLIST, count, recentlySoldFilters } = listRecentlySold;

  const [page, setPage] = useState(0);

  const [order, setOrder] = useState('desc');

  const [orderBy, setOrderBy] = useState('listed');

  const [rowsPerPage, setRowsPerPage] = useState(10);

  const [shownClients, setShownClients] = useState(0);

  const [csvLoading] = useState(false);

  const [recentlySoldLength, setRecentlySoldLength] = useState(0);

  useEffect(() => {
    if (RECENTLYSOLDLIST.length < recentlySoldLength) {
      setPage(0);
      setShownClients(0);
    }
    setRecentlySoldLength(RECENTLYSOLDLIST.length);
  }, [RECENTLYSOLDLIST, recentlySoldLength]);

  const handleRequestSort = (event, property) => {
    const isAsc = orderBy === property && order === 'asc';
    setOrder(isAsc ? 'desc' : 'asc');
    setOrderBy(property);
  };

  const handleChangePage = (event, newPage) => {
    // fetch new page if two away from needing to see new page
    if (((newPage + 2) * rowsPerPage) % 1000 === 0) {
      dispatch(recentlySoldAsync(((newPage + 2) * rowsPerPage) / 1000 + 1));
    }
    setPage(newPage);
  };
  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const [minPrice, setMinPrice] = useState('');
  const [maxPrice, setMaxPrice] = useState('');
  const [minYear, setMinYear] = useState('');
  const [maxYear, setMaxYear] = useState('');
  const [minDaysAgo, setMinDaysAgo] = useState('');
  const [maxDaysAgo, setMaxDaysAgo] = useState('');
  const [tagFilters, setTagFilters] = useState([]);
  const [zipCode, setZipCode] = useState('');
  const [city, setCity] = useState('');
  const [state, setState] = useState('');
  const [savedFilter, setSavedFilter] = useState('');
  const handleMinPriceChange = (newMinPrice) => {
    setMinPrice(newMinPrice);
    setSavedFilter('');
  };
  const handleMaxPriceChange = (newMaxPrice) => {
    setMaxPrice(newMaxPrice);
    setSavedFilter('');
  };
  const handleMinYearChange = (newMinYear) => {
    setMinYear(newMinYear);
    setSavedFilter('');
  };
  const handleMaxYearChange = (newMaxYear) => {
    setMaxYear(newMaxYear);
    setSavedFilter('');
  };
  const handleMinDaysAgoChange = (newMinDaysAgo) => {
    setMinDaysAgo(newMinDaysAgo);
    setSavedFilter('');
  };
  const handleMaxDaysAgoChange = (newMaxDaysAgo) => {
    setMaxDaysAgo(newMaxDaysAgo);
    setSavedFilter('');
  };
  const handleTagFiltersChange = (newTagFilters) => {
    setTagFilters(newTagFilters);
    setSavedFilter('');
  };
  const handleZipCodeChange = (newZipCode) => {
    setZipCode(newZipCode);
    setSavedFilter('');
  };
  const handleCityChange = (newCity) => {
    setCity(newCity);
    setSavedFilter('');
  };
  const handleStateChange = (newState) => {
    setState(newState);
    setSavedFilter('');
  };
  const handleSavedFilterChange = (newSavedFilter) => {
    setSavedFilter(newSavedFilter);
  };

  const exportCSV = () => {
    dispatch(getRecentlySoldCSV(minPrice, maxPrice, minYear, maxYear, minDaysAgo, maxDaysAgo, tagFilters));
  };

  const emptyRows = page > 0 ? Math.max(0, (1 + page) * rowsPerPage - RECENTLYSOLDLIST.length) : 0;
  const filteredRecentlySold = userInfo ? applySortFilter(RECENTLYSOLDLIST, getComparator(order, orderBy)) : [];
  // TODO, add val here to set length too
  useEffect(() => {
    setShownClients(count);
  }, [count]);

  return (
    <Page title="User" userInfo={userInfo}>
      <Container>
        {userInfo ? <RecentlySoldListCall /> : null}
        {userInfo && (
          <>
            <Stack direction="row" alignItems="center" justifyContent="space-between" mb={5}>
              <Typography variant="h4" gutterBottom>
                Welcome {userInfo.first_name.charAt(0).toUpperCase() + userInfo.first_name.slice(1)}{' '}
                {userInfo.last_name.charAt(0).toUpperCase() + userInfo.last_name.slice(1)} 👋
              </Typography>
            </Stack>
            <Card sx={{ marginBottom: '3%' }}>
              {loading ? (
                <Box sx={{ width: '100%' }}>
                  <LinearProgress />
                </Box>
              ) : null}
              {userInfo.company.recentlySoldPurchased ? (
                <Scrollbar>
                  <RecentlySoldListToolbar
                    recentlySoldFilters={recentlySoldFilters}
                    product={userInfo.company.product}
                    minPrice={minPrice}
                    setMinPrice={handleMinPriceChange}
                    maxPrice={maxPrice}
                    setMaxPrice={handleMaxPriceChange}
                    minYear={minYear}
                    setMinYear={handleMinYearChange}
                    maxYear={maxYear}
                    setMaxYear={handleMaxYearChange}
                    minDaysAgo={minDaysAgo}
                    setMinDaysAgo={handleMinDaysAgoChange}
                    maxDaysAgo={maxDaysAgo}
                    setMaxDaysAgo={handleMaxDaysAgoChange}
                    tagFilters={tagFilters}
                    setTagFilters={handleTagFiltersChange}
                    zipCode={zipCode}
                    setZipCode={handleZipCodeChange}
                    city={city}
                    setCity={handleCityChange}
                    state={state}
                    setState={handleStateChange}
                    savedFilter={savedFilter}
                    setSavedFilter={handleSavedFilterChange}
                  />
                  <TableContainer sx={{ minWidth: 800 }}>
                    <Table>
                      <ClientListHead
                        order={order}
                        orderBy={orderBy}
                        headLabel={TABLE_HEAD}
                        rowCount={rowsPerPage}
                        numSelected={0}
                        onRequestSort={handleRequestSort}
                        onSelectAllClick={handleRequestSort}
                        checkbox={0}
                      />
                      <TableBody>
                        {filteredRecentlySold.slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage).map((row) => {
                          const { id, address, city, state, zipCode, listed, price, year_built: yearBuilt } = row;

                          return (
                            <React.Fragment key={row.id}>
                              <TableRow hover key={id} tabIndex={-1} role="checkbox">
                                <TableCell component="th" scope="row" padding="normal">
                                  <Stack direction="row" alignItems="center" spacing={2}>
                                    <Typography variant="subtitle2" noWrap>
                                      {makeDate(listed.slice(0, 10))}
                                    </Typography>
                                  </Stack>
                                </TableCell>
                                <TableCell align="left">{address}</TableCell>
                                <TableCell align="left">{city}</TableCell>
                                <TableCell align="left">{state}</TableCell>
                                <TableCell align="left">{zipCode}</TableCell>
                                <TableCell align="left">{price.toLocaleString()}</TableCell>
                                <TableCell align="left">{yearBuilt}</TableCell>
                              </TableRow>
                            </React.Fragment>
                          );
                        })}
                        {emptyRows > 0 && (
                          <TableRow style={{ height: 53 * emptyRows }}>
                            <TableCell colSpan={6} />
                          </TableRow>
                        )}
                      </TableBody>

                      {filteredRecentlySold.length === 0 && (
                        <TableBody>
                          <TableRow>
                            <TableCell align="center" colSpan={6} sx={{ py: 3 }}>
                              <SearchNotFound searchQuery={''} tipe="client" />
                            </TableCell>
                          </TableRow>
                        </TableBody>
                      )}
                    </Table>
                  </TableContainer>
                </Scrollbar>
              ) : (
                <Box sx={{ p: 3, textAlign: 'center' }}>
                  <Typography variant="h5">Recently Sold Homes</Typography>
                  <Typography variant="subtitle2" gutterBottom>
                    You have not purchased this additional feature yet. You can add the option to get all recently sold
                    homes in your area by clicking the button below.
                  </Typography>
                  <Button variant="contained" color="primary" aria-label="Create Company" component="label">
                    <Link
                      href={`https://billing.stripe.com/p/login/aEU2aZ4PtbdD9A49AA?prefilled_email=${userInfo.company.email}`}
                      color="secondary"
                      underline="none"
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      Manage Subscription
                    </Link>
                  </Button>
                </Box>
              )}
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
            {/* TODO */}
            {csvLoading
              ? userInfo.status === 'admin' && (
                  <Button variant="contained">
                    <CircularProgress color="secondary" />
                  </Button>
                )
              : userInfo.status === 'admin' && (
                  <Button
                    onClick={exportCSV}
                    variant="contained"
                    component={RouterLink}
                    to="#"
                    startIcon={<Iconify icon="eva:plus-fill" />}
                  >
                    Download To CSV
                  </Button>
                )}
          </>
        )}
      </Container>
    </Page>
  );
}
