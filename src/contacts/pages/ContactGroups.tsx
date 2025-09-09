import React from 'react';
import {
  Grid,
  Card,
  CardContent,
  Typography,
  Box,
  IconButton,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Avatar,
  AvatarGroup,
} from '@mui/material';
import {
  Add,
  Edit,
  Delete,
  Group,
  People,
} from '@mui/icons-material';
import { motion } from 'framer-motion';
import { useNavigate } from 'react-router-dom';
import { PageHeader } from '@/components/core';
import { Button, LoadingSpinner } from '@/components/core';
import { useContactGroups, useDeleteContactGroup, useCreateContactGroup, useUpdateContactGroup } from '../api';
import { ContactGroupForm } from '../components/ContactGroupForm';
import { ContactGroup } from '../types';

const ContactGroups: React.FC = () => {
  const navigate = useNavigate();
  const [groupFormOpen, setGroupFormOpen] = React.useState(false);
  const [editingGroup, setEditingGroup] = React.useState<ContactGroup | null>(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = React.useState(false);
  const [groupToDelete, setGroupToDelete] = React.useState<ContactGroup | null>(null);

  const { data: groupsData, isLoading } = useContactGroups();
  const deleteGroupMutation = useDeleteContactGroup();
  const createGroupMutation = useCreateContactGroup();
  const updateGroupMutation = useUpdateContactGroup();

  const groups = groupsData?.results || [];

  const handleCreateGroup = () => {
    setEditingGroup(null);
    setGroupFormOpen(true);
  };

  const handleEditGroup = (group: ContactGroup) => {
    setEditingGroup(group);
    setGroupFormOpen(true);
  };

  const handleDeleteGroup = (group: ContactGroup) => {
    setGroupToDelete(group);
    setDeleteDialogOpen(true);
  };

  const confirmDelete = () => {
    if (groupToDelete) {
      deleteGroupMutation.mutate(groupToDelete.id, {
        onSuccess: () => {
          setDeleteDialogOpen(false);
          setGroupToDelete(null);
        },
      });
    }
  };

  const handleFormSubmit = (data: any) => {
    if (editingGroup) {
      updateGroupMutation.mutate(
        { id: editingGroup.id, data },
        {
          onSuccess: () => {
            setGroupFormOpen(false);
            setEditingGroup(null);
          },
        }
      );
    } else {
      createGroupMutation.mutate(data, {
        onSuccess: () => {
          setGroupFormOpen(false);
        },
      });
    }
  };

  if (isLoading) {
    return <LoadingSpinner message="Loading contact groups..." />;
  }

  return (
    <>
      <PageHeader
        title="Contact Groups"
        subtitle="Organize your contacts into groups"
        breadcrumbs={[
          { label: 'Contacts', href: '/contacts' },
          { label: 'Contact Groups' },
        ]}
        actions={
          <Button
            variant="contained"
            startIcon={<Add />}
            onClick={handleCreateGroup}
          >
            Create Group
          </Button>
        }
      />

      {groups.length === 0 ? (
        <Box
          display="flex"
          flexDirection="column"
          alignItems="center"
          justifyContent="center"
          minHeight="400px"
          textAlign="center"
        >
          <Group sx={{ fontSize: 64, color: 'text.secondary', mb: 2 }} />
          <Typography variant="h6" gutterBottom>
            No contact groups yet
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
            Create your first contact group to organize your contacts
          </Typography>
          <Button
            variant="contained"
            startIcon={<Add />}
            onClick={handleCreateGroup}
          >
            Create Your First Group
          </Button>
        </Box>
      ) : (
        <Grid container spacing={3}>
          {groups.map((group, index) => (
            <Grid item xs={12} sm={6} md={4} key={group.id}>
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
                  onClick={() => navigate(`/contacts/groups/${group.id}`)}
                >
                  <CardContent>
                    <Box display="flex" alignItems="center" justifyContent="space-between" mb={2}>
                      <Box display="flex" alignItems="center" gap={2}>
                        <Box
                          sx={{
                            width: 40,
                            height: 40,
                            backgroundColor: group.color,
                            borderRadius: 2,
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                          }}
                        >
                          <Typography variant="h6" color="white" fontWeight={600}>
                            {group.name.charAt(0).toUpperCase()}
                          </Typography>
                        </Box>
                        <Box>
                          <Typography variant="h6" fontWeight={600}>
                            {group.name}
                          </Typography>
                          <Typography variant="body2" color="text.secondary">
                            {group.contact_count} contact{group.contact_count !== 1 ? 's' : ''}
                          </Typography>
                        </Box>
                      </Box>
                      
                      <Box display="flex" gap={0.5}>
                        <Tooltip title="Edit Group">
                          <IconButton
                            size="small"
                            onClick={(e) => {
                              e.stopPropagation();
                              handleEditGroup(group);
                            }}
                          >
                            <Edit />
                          </IconButton>
                        </Tooltip>
                        <Tooltip title="Delete Group">
                          <IconButton
                            size="small"
                            onClick={(e) => {
                              e.stopPropagation();
                              handleDeleteGroup(group);
                            }}
                          >
                            <Delete />
                          </IconButton>
                        </Tooltip>
                      </Box>
                    </Box>

                    {group.description && (
                      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                        {group.description}
                      </Typography>
                    )}

                    {group.contacts.length > 0 && (
                      <Box display="flex" alignItems="center" gap={1}>
                        <AvatarGroup max={4} sx={{ '& .MuiAvatar-root': { width: 24, height: 24, fontSize: '0.75rem' } }}>
                          {group.contacts.slice(0, 4).map((contact) => (
                            <Avatar key={contact.id}>
                              {contact.first_name.charAt(0)}
                            </Avatar>
                          ))}
                        </AvatarGroup>
                        {group.contacts.length > 4 && (
                          <Typography variant="caption" color="text.secondary">
                            +{group.contacts.length - 4} more
                          </Typography>
                        )}
                      </Box>
                    )}
                  </CardContent>
                </Card>
              </motion.div>
            </Grid>
          ))}
        </Grid>
      )}

      {/* Contact Group Form Modal */}
      <ContactGroupForm
        open={groupFormOpen}
        onClose={() => {
          setGroupFormOpen(false);
          setEditingGroup(null);
        }}
        group={editingGroup}
        onSubmit={handleFormSubmit}
        loading={createGroupMutation.isPending || updateGroupMutation.isPending}
      />

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onClose={() => setDeleteDialogOpen(false)}>
        <DialogTitle>Delete Contact Group</DialogTitle>
        <DialogContent>
          <Typography>
            Are you sure you want to delete "{groupToDelete?.name}"? This action cannot be undone.
            Contacts in this group will not be deleted, only the group itself.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)} variant="outlined">
            Cancel
          </Button>
          <Button
            onClick={confirmDelete}
            variant="contained"
            color="error"
            loading={deleteGroupMutation.isPending}
          >
            Delete Group
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default ContactGroups;