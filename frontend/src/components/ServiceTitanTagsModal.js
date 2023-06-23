import React, { useState } from 'react';
import PropTypes from 'prop-types';
import * as Yup from 'yup';
import {
  Button,
  IconButton,
  TextField,
  Dialog,
  DialogContent,
  DialogTitle,
  Divider,
  Stack,
  Modal,
  Fade,
  Box,
  Typography,
} from '@mui/material';
import { useFormik, Form, FormikProvider } from 'formik';
import { useDispatch } from 'react-redux';

import { companyAsync } from '../redux/actions/authActions';
import Iconify from './Iconify';

ServiceTitanTagsModal.propTypes = {
  userInfo: PropTypes.objectOf(PropTypes.any),
};

export default function ServiceTitanTagsModal({ userInfo }) {
  const [open, setOpen] = useState(false);
  const [integrateInfo, setIntegrateInfo] = useState(false);

  const dispatch = useDispatch();
  const handleOpen = () => {
    setOpen(true);
  };
  const handleClose = () => {
    setOpen(false);
  };

  const IntegrateSTSchema = Yup.object().shape({
    forSale: Yup.string("'"),
    recentlySold: Yup.string(''),
    forSale_contacted: Yup.string(''),
    recentlySold_contacted: Yup.string(''),
  });

  const formik = useFormik({
    initialValues: {
      forSale: userInfo.company.service_titan_for_sale_tag_id ? userInfo.company.service_titan_for_sale_tag_id : '',
      forRent: '',
      recentlySold: userInfo.company.service_titan_recently_sold_tag_id
        ? userInfo.company.service_titan_recently_sold_tag_id
        : '',
      forSale_contacted: userInfo.company.service_titan_for_sale_contacted_tag_id
        ? userInfo.company.service_titan_for_sale_contacted_tag_id
        : '',
      recentlySold_contacted: userInfo.company.service_titan_sold_contacted_tag_id
        ? userInfo.company.service_titan_sold_contacted_tag_id
        : '',
    },
    validationSchema: IntegrateSTSchema,
    onSubmit: () => {
      setOpen(false);
      dispatch(
        companyAsync(
          '',
          '',
          '',
          '',
          '',
          values.forSale,
          values.forRent,
          values.recentlySold,
          values.forSale_contacted,
          values.recentlySold_contacted,
          ''
        )
      );
    },
  });

  const { errors, touched, values, handleSubmit, getFieldProps } = formik;
  return (
    <div>
      <Button variant="contained" color="primary" aria-label="Create Company" component="label" onClick={handleOpen}>
        {userInfo.company.service_titan_for_sale_tag_id ||
        userInfo.company.service_titan_for_rent_tag_id ||
        userInfo.company.service_titan_recently_sold_tag_id
          ? 'Edit'
          : 'Add'}{' '}
        Service Titan Tag IDs
      </Button>
      <IconButton onClick={() => setIntegrateInfo(true)}>
        <Iconify icon="bi:question-circle-fill" />
      </IconButton>
      <Dialog open={open} onClose={handleClose} sx={{ padding: '2px' }}>
        <DialogTitle>Service Titan Tag IDs</DialogTitle>
        <Divider />
        <DialogContent>
          <FormikProvider value={formik}>
            <Form autoComplete="off" noValidate onSubmit={handleSubmit}>
              <Stack spacing={3}>
                <TextField
                  fullWidth
                  label="For Sale ID"
                  placeholder="1234567890"
                  {...getFieldProps('forSale')}
                  error={Boolean(touched.forSale && errors.forSale)}
                  helperText={touched.forSale && errors.forSale}
                  sx={{ width: 400 }}
                />
                {/* <TextField
                                fullWidth
                                label="For Rent ID"
                                placeholder="1234567890"
                                {...getFieldProps('forRent')}
                                error={Boolean(touched.forRent && errors.forRent)}
                                helperText={touched.forRent && errors.forRent}
                            /> */}
                <TextField
                  fullWidth
                  label="Recently Sold ID"
                  placeholder="1234567890"
                  {...getFieldProps('recentlySold')}
                  error={Boolean(touched.recentlySold && errors.recentlySold)}
                  helperText={touched.recentlySold && errors.recentlySold}
                  sx={{ width: 400 }}
                />
                <TextField
                  fullWidth
                  label="For Sale and Contacted"
                  placeholder="1234567890"
                  {...getFieldProps('forSale_contacted')}
                  error={Boolean(touched.forSale_contacted && errors.forSale_contacted)}
                  helperText={touched.forSale_contacted && errors.forSale_contacted}
                  sx={{ width: 400 }}
                />
                <TextField
                  fullWidth
                  label="Recently Sold and Contacted"
                  placeholder="1234567890"
                  {...getFieldProps('recentlySold_contacted')}
                  error={Boolean(touched.recentlySold_contacted && errors.recentlySold_contacted)}
                  helperText={touched.recentlySold_contacted && errors.recentlySold_contacted}
                  sx={{ width: 400 }}
                />
              </Stack>
            </Form>
          </FormikProvider>
          <Stack direction="row" justifyContent="right">
            <Button color="error" onClick={handleClose}>
              Cancel
            </Button>
            <Button onClick={handleSubmit}>Submit</Button>
          </Stack>
        </DialogContent>
      </Dialog>
      <Modal
        open={integrateInfo}
        onClose={() => setIntegrateInfo(false)}
        closeAfterTransition
        aria-labelledby="modal-modal-title"
        aria-describedby="modal-modal-description"
        padding="10"
      >
        <Fade in={integrateInfo}>
          <Box
            sx={{
              position: 'absolute',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              width: 400,
              bgcolor: 'white',
              border: '2px solid #000',
              boxShadow: '24px',
              p: '4%',
            }}
          >
            <Typography id="modal-modal-title" variant="h5" component="h2">
              Add Service Titan Tag IDs Instructions
            </Typography>
            <Typography id="modal-modal-description" sx={{ mt: 2 }}>
              1. Adding Tag IDs will enable automatically updating your customer list on service titan with the
              information found from IMCM <br />
              <br />
              2. First to make a new tag, log in to service titan, go to settings, search for tag, and click tag types,
              then click add. <br />
              <br />
              3. Next choose a color, a name (like "For Sale or Home Listed"), then click save. <br />
              <br />
              4. You then need to scroll the list of tags to find the one you just made. Once found, click the edit
              button on the row of that tag. <br />
              <br />
              5. Copy the ID number from the URL. It will be a long number like 1234567890. <br />
              <br />
              <br />
              Note: You can make 2 different tags for this data or you can make one tag and use the same ID for all 2.
              <br />
              The last two tags are so you can keep track on Service Titan once you have marked a client as contacted
              within our system and differentiate your marketing campaigns with this data. Click{' '}
              <a href="https://www.loom.com/share/523b171ab81e47f2a050e1b28704c30e" target="_blank" rel="noreferrer">
                here{' '}
              </a>{' '}
              to see a video walking through this process.
            </Typography>
          </Box>
        </Fade>
      </Modal>
    </div>
  );
}
