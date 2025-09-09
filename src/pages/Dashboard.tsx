import React from 'react';
import { Grid, Card, CardContent, Typography, Box } from '@mui/material';
import { motion } from 'framer-motion';
import { PageHeader } from '@/components/core';
import { Event, Schedule, TrendingUp, People } from '@mui/icons-material';

const Dashboard: React.FC = () => {
  const stats = [
    {
      title: 'Total Bookings',
      value: '1,234',
      change: '+12%',
      icon: Event,
      color: 'primary.main',
    },
    {
      title: 'This Week',
      value: '89',
      change: '+5%',
      icon: Schedule,
      color: 'success.main',
    },
    {
      title: 'Conversion Rate',
      value: '68%',
      change: '+3%',
      icon: TrendingUp,
      color: 'info.main',
    },
    {
      title: 'Active Contacts',
      value: '456',
      change: '+8%',
      icon: People,
      color: 'secondary.main',
    },
  ];

  return (
    <>
      <PageHeader
        title="Dashboard"
        subtitle="Welcome back! Here's what's happening with your scheduling."
      />
      
      <Grid container spacing={3}>
        {stats.map((stat, index) => (
          <Grid item xs={12} sm={6} md={3} key={stat.title}>
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.3, delay: index * 0.1 }}
            >
              <Card>
                <CardContent>
                  <Box display="flex" alignItems="center" justifyContent="space-between">
                    <Box>
                      <Typography color="text.secondary" gutterBottom variant="overline">
                        {stat.title}
                      </Typography>
                      <Typography variant="h4" component="div" sx={{ fontWeight: 700 }}>
                        {stat.value}
                      </Typography>
                      <Typography variant="body2" sx={{ color: 'success.main', fontWeight: 500 }}>
                        {stat.change} from last month
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
      </Grid>
    </>
  );
};

export default Dashboard;