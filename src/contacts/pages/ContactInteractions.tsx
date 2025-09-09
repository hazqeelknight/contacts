import React from 'react';
import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Typography,
  Box,
  TextField,
  MenuItem,
  Chip,
  Avatar,
} from '@mui/material';
import { History, Person, Event } from '@mui/icons-material';
import { motion } from 'framer-motion';
import { useNavigate } from 'react-router-dom';
import { PageHeader } from '@/components/core';
import { LoadingSpinner } from '@/components/core';
import { useContactInteractions } from '../api';
import { formatRelativeTime } from '@/utils/formatters';

const INTERACTION_TYPE_OPTIONS = [
  { value: '', label: 'All Types' },
  { value: 'booking_created', label: 'Booking Created' },
  { value: 'booking_completed', label: 'Booking Completed' },
  { value: 'booking_cancelled', label: 'Booking Cancelled' },
  { value: 'email_sent', label: 'Email Sent' },
  { value: 'note_added', label: 'Note Added' },
  { value: 'manual_entry', label: 'Manual Entry' },
];

const ContactInteractions: React.FC = () => {
  const navigate = useNavigate();
  const [page, setPage] = React.useState(0);
  const [rowsPerPage, setRowsPerPage] = React.useState(20);
  const [typeFilter, setTypeFilter] = React.useState('');

  const { data: interactionsData, isLoading } = useContactInteractions();
  const interactions = interactionsData?.results || [];
  const totalInteractions = interactionsData?.count || 0;

  // Filter interactions by type
  const filteredInteractions = typeFilter
    ? interactions.filter(interaction => interaction.interaction_type === typeFilter)
    : interactions;

  if (isLoading) {
    return <LoadingSpinner message="Loading interactions..." />;
  }

  return (
    <>
      <PageHeader
        title="Contact Interactions"
        subtitle="View the history of sent notifications"
      />
      
      {/* Filters */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Box display="flex" gap={2} alignItems="center">
          <TextField
            select
            size="small"
            label="Interaction Type"
            value={typeFilter}
            onChange={(e) => setTypeFilter(e.target.value)}
            sx={{ minWidth: 200 }}
          >
            {INTERACTION_TYPE_OPTIONS.map((option) => (
              <MenuItem key={option.value} value={option.value}>
                {option.label}
              </MenuItem>
            ))}
          </TextField>
          
          <Typography variant="body2" color="text.secondary">
            {filteredInteractions.length} interaction{filteredInteractions.length !== 1 ? 's' : ''}
          </Typography>
        </Box>
      </Paper>

      {/* Interactions Table */}
      <Paper>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Contact</TableCell>
                <TableCell>Type</TableCell>
                <TableCell>Description</TableCell>
                <TableCell>Related Booking</TableCell>
                <TableCell>Date</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {filteredInteractions
                .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                .map((interaction, index) => (
                <motion.tr
                  key={interaction.id}
                  component={TableRow}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.2, delay: index * 0.05 }}
                  hover
                  sx={{ cursor: 'pointer' }}
                  onClick={() => {
                    // Navigate to contact detail if we had contact ID
                    // For now, we only have contact_name from the serializer
                  }}
                >
                  <TableCell>
                    <Box display="flex" alignItems="center" gap={2}>
                      <Avatar sx={{ width: 32, height: 32 }}>
                        <Person />
                      </Avatar>
                      <Typography variant="subtitle2">
                        {interaction.contact_name}
                      </Typography>
                    </Box>
                  </TableCell>
                  
                  <TableCell>
                    <Chip
                      label={interaction.interaction_type_display}
                      size="small"
                      variant="outlined"
                      color={
                        interaction.interaction_type.includes('booking') ? 'primary' :
                        interaction.interaction_type === 'email_sent' ? 'info' :
                        interaction.interaction_type === 'note_added' ? 'success' :
                        'default'
                      }
                    />
                  </TableCell>
                  
                  <TableCell>
                    <Typography variant="body2">
                      {interaction.description}
                    </Typography>
                  </TableCell>
                  
                  <TableCell>
                    {interaction.booking_id ? (
                      <Chip
                        label="View Booking"
                        size="small"
                        color="primary"
                        variant="outlined"
                        icon={<Event />}
                        onClick={(e) => {
                          e.stopPropagation();
                          navigate(`/events/bookings/${interaction.booking_id}`);
                        }}
                        sx={{ cursor: 'pointer' }}
                      />
                    ) : (
                      <Typography variant="body2" color="text.secondary">
                        -
                      </Typography>
                    )}
                  </TableCell>
                  
                  <TableCell>
                    <Typography variant="body2">
                      {formatRelativeTime(interaction.created_at)}
                    </Typography>
                  </TableCell>
                </motion.tr>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
        
        <TablePagination
          component="div"
          count={filteredInteractions.length}
          page={page}
          onPageChange={(_, newPage) => setPage(newPage)}
          rowsPerPage={rowsPerPage}
          onRowsPerPageChange={(e) => {
            setRowsPerPage(parseInt(e.target.value, 10));
            setPage(0);
          }}
          rowsPerPageOptions={[10, 20, 50, 100]}
        />
      </Paper>

      {filteredInteractions.length === 0 && (
        <Box
          display="flex"
          flexDirection="column"
          alignItems="center"
          justifyContent="center"
          minHeight="300px"
          textAlign="center"
        >
          <History sx={{ fontSize: 64, color: 'text.secondary', mb: 2 }} />
          <Typography variant="h6" gutterBottom>
            No interactions found
          </Typography>
          <Typography variant="body2" color="text.secondary">
            {typeFilter ? 'Try changing the filter to see more interactions' : 'Interactions will appear here as you engage with contacts'}
          </Typography>
        </Box>
      )}
    </>
  );
};

export default ContactInteractions;