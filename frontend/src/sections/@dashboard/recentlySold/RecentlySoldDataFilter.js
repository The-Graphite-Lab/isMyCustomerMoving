import React, { useEffect, useState } from 'react';
import { useDispatch } from 'react-redux';

import { makeStyles } from '@mui/styles';
import {
  Box,
  Button,
  Checkbox,
  FormControl,
  FormControlLabel,
  FormLabel,
  Grid,
  Input,
  InputLabel,
  MenuItem,
  Select,
  Typography,
  Tooltip,
  IconButton,
  Dialog,
  DialogContent,
  DialogTitle,
  Divider,
  Stack
} from '@mui/material';


import Iconify from '../../../components/Iconify';
import { filterRecentlySoldAsync, recentlySoldAsync } from '../../../redux/actions/usersActions'

const useStyles = makeStyles((theme) => ({
  root: {
    position: 'relative',
  },
  filterButton: {
    position: 'absolute',
    right: theme.spacing(1),
    top: theme.spacing(1),
  },
  filterBox: {
    position: 'absolute',
    margin: 'auto',
    maxHeight: '50vh',
    overflowY: 'auto',
    backgroundColor: theme.palette.background.paper,
    boxShadow: theme.shadows[5],
    padding: theme.spacing(2),
    width: '30%',
    minWidth: '500px',
  },
}));




export default function RecentlySoldDataFilter({minPrice, setMinPrice: handleChangeMinPrice, maxPrice, setMaxPrice: handleChangeMaxPrice,
                                                    minYear, setMinYear: handleChangeMinYear, maxYear, setMaxYear: handleChangeMaxYear,
                                                    minDaysAgo, setMinDaysAgo: handleChangeMinDaysAgo, maxDaysAgo, setMaxDaysAgo: handleChangeMaxDaysAgo,}) {
    const classes = useStyles();
    const [showFilters, setShowFilters] = useState(false);
    const [showClearFilters, setShowClearFilters] = useState(false);
    const [tagFilters, setTagFilters] = useState([]);
    const [error, setError] = useState('');
    const dispatch = useDispatch();

    const handleTagFilterChange = (event) => {
        const { value } = event.target;
        setTagFilters((prevFilters) => {
        if (prevFilters.includes(value)) {
            return prevFilters.filter((filter) => filter !== value);
        } 
        return [...prevFilters, value];
        
        });
    };

    useEffect(() => {
        if (minPrice || maxPrice || minYear || maxYear || minDaysAgo !== 0 || maxDaysAgo !== 30 || tagFilters.length > 0) {
            setShowClearFilters(true);
        } else {
            setShowClearFilters(false);
        }
    }, [minPrice, maxPrice, minYear, maxYear, minDaysAgo, maxDaysAgo, tagFilters]);


    const handleClearFilters = () => {
        handleChangeMinPrice('');
        handleChangeMaxPrice('');
        handleChangeMinYear('');
        handleChangeMaxYear('');
        handleChangeMinDaysAgo(0);
        handleChangeMaxDaysAgo(30);
        setTagFilters([]);
        setError('');
        dispatch(recentlySoldAsync(1));
    };

    const tagOptions = [
        { value: 'Solar', label: 'Solar' },
        { value: 'Well Water', label: 'Well Water' },
        { value: 'Residential', label: 'Residential' },
        { value: 'Pool', label: 'Pool' },
        { value: 'Commercial', label: 'Commercial' },
        { value: 'Fixer Upper', label: 'Fixer Upper' },
    ];

    const handleDaysAgoChange = (event, type) => {
        const value = event.target.value;
        if (value < 0) {
            if (type === 'min') {
                handleChangeMinDaysAgo(0);                
            } else {
                handleChangeMaxDaysAgo(0);                
            }
        } else if (value > 30) {
            if (type === 'min') {
                handleChangeMinDaysAgo(30);                
            } else {
                handleChangeMaxDaysAgo(30);                
            }
        } else {
            /* eslint-disable no-lonely-if */
            if (type === 'min') {
                handleChangeMinDaysAgo(value);                
            } else {
                handleChangeMaxDaysAgo(value);                
            }
        }        
    };

    const handleFilterSubmit = (event) => {
        event.preventDefault();
        // Filter data based on selected filters
        if( minDaysAgo > maxDaysAgo || maxDaysAgo < minDaysAgo) {
            setError('Min days ago sold must be less than max days ago sold')
        } else {
            dispatch(filterRecentlySoldAsync(minPrice, maxPrice, minYear, maxYear, minDaysAgo, maxDaysAgo, tagFilters))
            setShowFilters(false);
        }
    };

    return (
        <div className={classes.root}>
            <Stack direction="row" spacing={2} alignItems="space-between">
                <Tooltip title="Filter list">
                    <IconButton onClick={()=>setShowFilters(true)}>
                        <Iconify icon="ic:round-filter-list" />
                    </IconButton>
                </Tooltip>
                {showClearFilters && (
                    <Tooltip title="Clear filters">
                        <IconButton onClick={handleClearFilters}>
                            <Iconify icon="ic:baseline-clear" />
                        </IconButton>
                    </Tooltip>
                )}
            </Stack>
            {showFilters && (
            <Dialog sx={{padding:"200px"}} className={classes.filterBox} open={showFilters} onClose={()=>(setShowFilters(false))} >
                <DialogTitle>Filter List</DialogTitle>
                <Divider />
                <DialogContent>
                <form onSubmit={handleFilterSubmit}>
                    <Box mb={2}>
                        <Typography variant="h5">Select Filters</Typography>                        
                    </Box>
                    <Grid container spacing={2}>
                        <Grid item xs={12}>
                            <Tooltip title="This will filter for the price that the house was either sold or listed for">
                                <Box mt={2}>
                                    <Typography variant="h6" mb={2}>Housing Price</Typography>
                                    <Stack direction="row" spacing={2} alignItems="center">
                                        <FormControl fullWidth>
                                            <InputLabel>Min Price</InputLabel>
                                            <Input
                                                type="number"
                                                value={minPrice}
                                                onChange={(event) => handleChangeMinPrice(event.target.value)}
                                            />
                                        </FormControl>
                                        <FormControl fullWidth>
                                            <InputLabel>Max Price</InputLabel>
                                            <Input
                                                type="number"
                                                value={maxPrice}
                                                onChange={(event) => handleChangeMaxPrice(event.target.value)}
                                            />
                                        </FormControl>
                                    </Stack>
                                </Box>
                            </Tooltip>
                        </Grid>                        
                        <Grid item xs={12}>
                            <Tooltip title="How long ago was the house sold, this data only goes back 30 days">
                                <Box mt={2}>
                                    <Typography variant="h6" mb={2}>Days Ago Sold</Typography>
                                    {error && (
                                        <Grid item xs={12}>
                                            <Typography color="error">{error}</Typography>
                                        </Grid>
                                    )}
                                    <Stack direction="row" spacing={2} alignItems="center">
                                        <FormControl fullWidth>
                                            <InputLabel>Minimum</InputLabel>
                                            <Input
                                                type="number"
                                                value={minDaysAgo}
                                                onChange={(event) => handleDaysAgoChange(event, 'min')}
                                            />
                                        </FormControl>
                                        <FormControl fullWidth>
                                            <InputLabel>Maximum</InputLabel>
                                            <Input
                                                type="number"
                                                value={maxDaysAgo}
                                                onChange={(event) => handleDaysAgoChange(event, 'max')}
                                            />
                                        </FormControl>
                                    </Stack>
                                </Box>
                            </Tooltip>
                        </Grid>
                        <Grid item xs={12}>
                            <Tooltip title="Year the house was built">
                                <Box mt={2}>
                                    <Typography variant="h6" mb={2}>Year Built</Typography>                                    
                                    <Stack direction="row" spacing={2} alignItems="center">
                                        <FormControl fullWidth>
                                            <InputLabel>Min Year Built</InputLabel>
                                            <Input
                                                type="number"
                                                value={minYear}
                                                onChange={(event) => handleChangeMinYear(event.target.value)}
                                            />
                                        </FormControl>
                                        <FormControl fullWidth>
                                            <InputLabel>Max Year Built</InputLabel>
                                            <Input
                                                type="number"
                                                value={maxYear}
                                                onChange={(event) => handleChangeMaxYear(event.target.value)}
                                            />
                                        </FormControl>
                                    </Stack>
                                </Box>
                            </Tooltip>
                        </Grid>                                                
                    {/* <Grid item xs={12}>
                        <FormControl component="fieldset">
                        <FormLabel component="legend">Tags</FormLabel>
                        <Grid container spacing={1}>
                            {tagOptions.map((option) => (
                                <FormControlLabel
                                key={option.value}
                                control={
                                    <Checkbox
                                    checked={tagFilters.includes(option.value)}
                                    onChange={handleTagFilterChange}
                                    value={option.value}
                                    />
                                }
                                label={option.label}
                                />
                            ))}
                           
                        </Grid>
                        </FormControl>
                    </Grid> */}
                </Grid>
                <Box mt={2}>
                    <Button type="submit" variant="contained" color="primary">
                        Apply Filters
                    </Button>
                </Box>
                </form>
                </DialogContent>
            </Dialog>
        )}
            


        </div>
    )
}