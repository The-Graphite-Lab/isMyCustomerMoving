// component
import Iconify from '../../components/Iconify';

// ----------------------------------------------------------------------

const getIcon = (name) => <Iconify icon={name} width={22} height={22} />;

const navConfig = [
  {
    title: 'Customer Data',
    path: '/dashboard/customers',
    icon: getIcon('eva:people-fill'),
  },
  {
    title: 'Referrals',
    path: '/dashboard/referrals',
    icon: getIcon('ri:share-forward-fill'),
  },
  {
    title: 'Recently Sold Data',
    path: '/dashboard/recentlysold',
    icon: getIcon('material-symbols:house'),
  },
  {
    title: 'Settings',
    path: '/dashboard/settings',
    icon: getIcon('ri:user-settings-fill'),
  },
];

export default navConfig;
