Frontend Tests 28 June 2023

## End to End tests
 
#### Auth

- [x] Auth
  - [x] Log in correct email + password
  - [x] Fail login w/incorrect password 
- [x] Log out
- [ ] Log in using google SSO (not working)

#### Routes
- [ ] /dashboard
- [ ] /dashboard/referrals
- [ ] /dashboard/recentlysold
- [ ] /dashboard/settings/user
- [ ] /dashboard/settings/enterprise
- [ ] Protected routes (ensure these require auth)
  - [ ] /dashboard

#### Profile Settings

- [ ] Edit first name
- [ ] Edit last name
- [ ] Edit email
- [ ] Edit phone number
- [ ] Add User
  - [ ] Send Reminder Email
  - [ ] Make User Admin
  - [ ] Delete user
  - [ ] Delete multiple users
- [ ] Connect To Service Titan
  - [ ] Add Tenant ID
  - [ ] Then should see button to add client ID and secret
    - [ ] Add client ID and secret
  - [ ] Then should be able to add tag IDs
    - [ ] Add tag IDs
  - [ ] Then should be able to EDIT tag IDs

#### Dashboard

- [ ] Show 10 per page, 50, 100 clients
- [ ] View more than 1000 clients
- [ ] Make note
- [ ] Set as contacted/not contacted
  - [ ] This is not updating automatically on the site. You pretty much have to refresh the page to get the clients again and then it is accurate
  - [ ] Ensure this updates the numbers properly
  - [ ] Make sure cannot see contacted checkbox if status is “No Change”
- [ ] Delete a client
- [ ] Delete multiple clients
- [ ] Search by name
- [ ] Search by address
- [ ] View map
- [ ] Filter by
  - [ ] For Sale
  - [ ] Recently Sold
  - [ ] Off Market (No Change)
  - [ ] Zip
  - [ ] City
  - [ ] State
  - [ ] Min/max price
  - [ ] Min/max housing built year
  - [ ] Min/max customer since date
  - [ ] Tags
  - [ ] Equipment Install Date (Not Tested Yet)
- [ ] Download all to CSV
  - [ ] Download all
  - [ ] Download filtered list
- [ ] Upload Clients with File (Not Tested Yet)
- [ ] Sync Clients with service titan (Must have added service titan integration in order to see this)
- [ ] Check for all three options

#### Recently sold data
  - Show 10 per page, 50, 100
  - View more than 1000 clients
  - Download (TODO: not currently auto filtering for less than 30 days)
    - All
    - Filtered
  - Filter by
    - Saved Filter
    - Zip
    - City
    - State
    - Min/Max Housing Price
    - Min/Max year built
    - Tags
    - Min/Max days ago
  - Save Filter
    - Create a new filter, with and without zapier
    - Test the new filter
- All Two Factor Auth (no one uses this, has not been manually tested)
- Active vs admin user
  - Can only see clients with status
  - Can not download anything
  - Can not upload anything
  - Can not add users, delete users, or change status of users
- Free tier vs not free tier shows pop up’s and alerts as expected
