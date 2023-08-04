Frontend Tests 28 June 2023

## End to End tests
 
 This CRA app uses Playwright for end-to-end testing.  Below is a list of the completed and to be completed tests.

#### Auth

- [x] Auth
  - [x] Log in correct email + password
  - [x] Fail login w/incorrect password 
- [x] Log out
- [ ] Log in using google SSO (not working)

#### Routes
- [x] /dashboard
- [x] /dashboard/customers
- [x] /dashboard/referrals
- [x] /dashboard/forsale
- [x] /dashboard/recentlysold
- [x] /dashboard/settings/user
- [x] /dashboard/settings/enterprise
- [x] Protected routes (ensure these require auth)

#### Profile Settings

- [x] Edit first name
- [x] Edit last name
- [ ] Edit email
- [x] Edit phone number
- [x] Add User
  - [x] Email input validation
  - [x] Send Reminder Email
  - [x] Make User Admin
  - [x] Delete user

*gonna need your help here*
**REID: You can use fake numbers for all of this, nothing here is actually testing a connection**
- [ ] Connect To Service Titan
  - [ ] Add Tenant ID
  - [ ] Then should see button to add client ID and secret
    - [ ] Add client ID and secret
  - [ ] Then should be able to add tag IDs
    - [ ] Add tag IDs
  - [ ] Then should be able to EDIT tag IDs

#### Dashboard

- [x] Show 10 per page, 50, 100 clients
- [ ] View more than 1000 clients  
**how should I test this?  Just click through pages?** 
**REID: YES**
- [x] Make note
- [x] Set as contacted/not contacted
  
  **we can make notes async so we don't have to reload.  Worth the effort?**
  **REID: Not at this time because I don't think hardly anyone uses the note feature**
  - [ ] This is not updating automatically on the site. You pretty much have to refresh the page to get the clients again and then it is accurate

  **what do you mean "update numbers properly"**
  **REID: When you press contacted, if the status of that client is "House For Sale" then the large number at the top**
  **should decrease by one. This is the same for Recently Sold. Then if you click them to be uncontacted, then**
  **the number should increase by one.**
  - [ ] Ensure this updates the numbers properly

  **what do you mean "update numbers properly"**

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
- [ ] Show 10 per page, 50, 100
- [ ] View more than 1000 clients
- [ ] Download (TODO: not currently auto filtering for less than 30 days)
  - [ ] All
  - [ ] Filtered
- [ ] Filter by
  - [ ] Saved Filter
  - [ ] Zip
  - [ ] City
  - [ ] State
  - [ ] Min/Max Housing Price
  - [ ] Min/Max year built
  - [ ] Tags
  - [ ] Min/Max days ago
- [ ] Save Filter
  - [ ] Create a new filter, with and without zapier
  - [ ] Test the new filter
- [ ] All Two Factor Auth (no one uses this, has not been manually tested)
- [ ] Active vs admin user
  - [ ] Can only see clients with status
  - [ ] Can not download anything
  - [ ] Can not upload anything
  - [ ] Can not add users, delete users, or change status of users
- [ ] Free tier vs not free tier shows pop up’s and alerts as expected
