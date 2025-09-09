import React from 'react';
import {
  Grid,
  Card,
  CardContent,
  Typography,
  Box,
  List,
  ListItem,
  ListItemText,
  Chip,
} from '@mui/material';
import {
  People,
  PersonAdd,
  Group,
  TrendingUp,
  Business,
} from '@mui/icons-material';
import { motion } from 'framer-motion';
import { useNavigate } from 'react-router-dom';
import { PageHeader } from '@/components/core';
import { Button } from '@/components/core';
import { useContactStats } from '../api';
import { LoadingSpinner } from '@/components/core';

const ContactsOverview: React.FC = () => {
  const navigate = useNavigate();
  const { data: stats, isLoading } = useContactStats();

  if (isLoading) {
    return <LoadingSpinner message="Loading contacts overview..." />;
  }

  const statCards = [
    {
      title: 'Total Contacts',
      value: stats?.total_contacts || 0,
      icon: People,
      color: 'primary.main',
      action: () => navigate('/contacts/list'),
    },
    {
      title: 'Active Contacts',
      value: stats?.active_contacts || 0,
      icon: PersonAdd,
      color: 'success.main',
      action: () => navigate('/contacts/list?is_active=true'),
    },
    {
      title: 'Contact Groups',
      value: stats?.total_groups || 0,
      icon: Group,
      color: 'info.main',
      action: () => navigate('/contacts/groups'),
    },
    {
      title: 'Recent Interactions',
      value: stats?.recent_interactions || 0,
      icon: TrendingUp,
      color: 'secondary.main',
      action: () => navigate('/contacts/interactions'),
    },
  ];

  return (
    <>
      <PageHeader
        title="Contacts Overview"
        subtitle="Manage your contact database and interactions"
        actions={
          <Box display="flex" gap={1}>
            <Button
              variant="outlined"
              onClick={() => navigate('/contacts/list')}
            >
              View All Contacts
            </Button>
            <Button
              variant="contained"
              startIcon={<PersonAdd />}
              onClick={() => navigate('/contacts/list')}
            >
              Add Contact
            </Button>
          </Box>
        }
      />
      
      <Grid container spacing={3}>
        {/* Statistics Cards */}
        {statCards.map((stat, index) => (
          <Grid item xs={12} sm={6} md={3} key={stat.title}>
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.3, delay: index * 0.1 }}
            >
              <Card
                sx={{
                  cursor: 'pointer',
                  transition: 'all 0.2s',
                  '&:hover': {
                    transform: 'translateY(-4px)',
                    boxShadow: 4,
                  },
                }}
                onClick={stat.action}
              >
                <CardContent>
                  <Box display="flex" alignItems="center" justifyContent="space-between">
                    <Box>
                      <Typography color="text.secondary" gutterBottom variant="overline">
                        {stat.title}
                      </Typography>
                      <Typography variant="h4" component="div" sx={{ fontWeight: 700 }}>
                        {stat.value.toLocaleString()}
                      </Typography>
                    </Box>
                    <Box
                      sx={{
                        backgroundColor: stat.color,
                        borderRadius: 2,
                        p: 1.5,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                      }}
                    >
                      <stat.icon sx={{ color: 'white', fontSize: 24 }} />
                    </Box>
                  </Box>
                </CardContent>
              </Card>
            </motion.div>
          </Grid>
        ))}

        {/* Top Companies */}
        <Grid item xs={12} md={6}>
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.3, delay: 0.4 }}
          >
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" gap={1} mb={2}>
                  <Business color="primary" />
                  <Typography variant="h6">Top Companies</Typography>
                </Box>
                
                {stats?.top_companies && stats.top_companies.length > 0 ? (
                  <List dense>
                    {stats.top_companies.map((company, index) => (
                      <ListItem key={index} sx={{ px: 0 }}>
                        <ListItemText
                          primary={company.company}
                          secondary={`${company.count} contact${company.count !== 1 ? 's' : ''}`}
                        />
                        <Chip
                          label={company.count}
                          size="small"
                          color="primary"
                          variant="outlined"
                        />
                      </ListItem>
                    ))}
                  </List>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No company data available
                  </Typography>
                )}
              </CardContent>
            </Card>
          </motion.div>
        </Grid>

        {/* Booking Frequency */}
        <Grid item xs={12} md={6}>
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.3, delay: 0.5 }}
          >
            <Card>
              <CardContent>
                <Box display="flex" alignItems="center" gap={1} mb={2}>
                  <TrendingUp color="primary" />
                  <Typography variant="h6">Booking Activity</Typography>
                </Box>
                
                {stats?.booking_frequency && (
                  <Box>
                    <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
                      <Typography variant="body2">This Month</Typography>
                      <Chip
                        label={stats.booking_frequency.this_month}
                        size="small"
                        color="success"
                      />
                    </Box>
                    <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
                      <Typography variant="body2">Last Month</Typography>
                      <Chip
                        label={stats.booking_frequency.last_month}
                        size="small"
                        color="info"
                      />
                    </Box>
                    <Box display="flex" justifyContent="space-between" alignItems="center">
                      <Typography variant="body2">This Year</Typography>
                      <Chip
                        label={stats.booking_frequency.this_year}
                        size="small"
                        color="primary"
                      />
                    </Box>
                  </Box>
                )}
              </CardContent>
            </Card>
          </motion.div>
        </Grid>
      </Grid>
    </>
  );
};

export default ContactsOverview;