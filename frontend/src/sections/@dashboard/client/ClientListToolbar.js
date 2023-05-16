import { useState, useEffect } from 'react';
import PropTypes from 'prop-types';
import {useDispatch } from 'react-redux';
// material
import { styled } from '@mui/material/styles';
import { Toolbar, Tooltip, IconButton, Typography, OutlinedInput, InputAdornment, Stack, Button } from '@mui/material';

// component
import Iconify from '../../../components/Iconify';
import CustomerDataFilter  from './CustomerDataFilter';
// redux
import { deleteClientAsync } from '../../../redux/actions/usersActions';

// ----------------------------------------------------------------------

const RootStyle = styled(Toolbar)(({ theme }) => ({
  height: 96,
  display: 'flex',
  justifyContent: 'space-between',
  padding: theme.spacing(0, 1, 0, 3),
}));

const SearchStyle = styled(OutlinedInput)(({ theme }) => ({
  width: 240,
  transition: theme.transitions.create(['box-shadow', 'width'], {
    easing: theme.transitions.easing.easeInOut,
    duration: theme.transitions.duration.shorter,
  }),
  '&.Mui-focused': { width: 320, boxShadow: theme.customShadows.z8 },
  '& fieldset': {
    borderWidth: `1px !important`,
    borderColor: `${theme.palette.grey[500_32]} !important`,
  },
}));

// ----------------------------------------------------------------------

ClientListToolbar.propTypes = {
  numSelected: PropTypes.number,
  filterName: PropTypes.string,
  onFilterName: PropTypes.func,
  selectedClients: PropTypes.array,
  product: PropTypes.string,
  minPrice: PropTypes.string,
  setMinPrice: PropTypes.func,
  maxPrice: PropTypes.string,
  setMaxPrice: PropTypes.func,
  minYear: PropTypes.string,
  setMinYear: PropTypes.func,
  maxYear: PropTypes.string,
  setMaxYear: PropTypes.func,
  equipInstallDateMin: PropTypes.string,
  setEquipInstallDateMin: PropTypes.func,
  equipInstallDateMax: PropTypes.string,
  setEquipInstallDateMax: PropTypes.func,
  statusFilters: PropTypes.array,
  setStatusFilters: PropTypes.func,
  listOrMap: PropTypes.string,
  setListOrMap: PropTypes.func,

};

export default function ClientListToolbar({ numSelected, filterName, onFilterName, selectedClients, product, 
                                            minPrice, setMinPrice, maxPrice, setMaxPrice, minYear, setMinYear, maxYear, setMaxYear,
                                            equipInstallDateMin, setEquipInstallDateMin, equipInstallDateMax, setEquipInstallDateMax,
                                            statusFilters, setStatusFilters, listOrMap, setListOrMap }) {
  const dispatch = useDispatch();


  const clickDelete = (event, clients) => {
    dispatch(deleteClientAsync(clients));
    const timer = Math.ceil(clients.length / 1000)*250;
    setTimeout(() => {
     window.location.reload();
    }, timer);

  };

  const handleClickList = () => {
    setListOrMap('list');
  };

  const handleClickMap = () => {
    setListOrMap('map');
  };

  return (
    <RootStyle
      sx={{
        ...(numSelected > 0 && {
          color: 'primary.main',
          bgcolor: 'primary.lighter',
        }),
      }}
    >
      {numSelected > 0 ? (
        <Typography component="div" variant="subtitle1">
          {numSelected} selected
        </Typography>
      ) : (
        <Stack direction="row" alignItems="center" spacing={1}>
          <Button onClick={handleClickList} variant={listOrMap === 'list' ? 'contained' : 'outlined'}>
            List
          </Button>
          <Button onClick={handleClickMap} variant={listOrMap === 'map' ? 'contained' : 'outlined'}>
            Map
          </Button>
          <SearchStyle
            value={filterName}
            onChange={onFilterName}
            placeholder="Search user..."
            startAdornment={
              <InputAdornment position="start">
                <Iconify icon="eva:search-fill" sx={{ color: 'text.disabled', width: 20, height: 20 }} />
              </InputAdornment>
            }
          />
        </Stack>
      )}

      {numSelected > 0 ? (
        <Tooltip title="Delete">
          <IconButton onClick={(event)=>clickDelete(event, selectedClients)}>
            <Iconify icon="eva:trash-2-fill" />
          </IconButton>
        </Tooltip>
      ) : (
        <CustomerDataFilter
          product={product}
          minPrice={minPrice}
          setMinPrice={setMinPrice}
          maxPrice={maxPrice}
          setMaxPrice={setMaxPrice}
          minYear={minYear}
          setMinYear={setMinYear}
          maxYear={maxYear}
          setMaxYear={setMaxYear}
          equipInstallDateMin={equipInstallDateMin}
          setEquipInstallDateMin={setEquipInstallDateMin}
          equipInstallDateMax={equipInstallDateMax}
          setEquipInstallDateMax={setEquipInstallDateMax}
          statusFilters={statusFilters}
          setStatusFilters={setStatusFilters}
        />
      )}
    </RootStyle>
  );
}
