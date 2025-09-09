from rest_framework.permissions import BasePermission


class CanViewContacts(BasePermission):
    """Permission to view contacts."""
    
    def has_permission(self, request, view):
        return request.user.has_perm('contacts.view_contact')


class CanAddContacts(BasePermission):
    """Permission to add contacts."""
    
    def has_permission(self, request, view):
        return request.user.has_perm('contacts.add_contact')


class CanChangeContacts(BasePermission):
    """Permission to change contacts."""
    
    def has_permission(self, request, view):
        return request.user.has_perm('contacts.change_contact')


class CanDeleteContacts(BasePermission):
    """Permission to delete contacts."""
    
    def has_permission(self, request, view):
        return request.user.has_perm('contacts.delete_contact')


class CanManageContacts(BasePermission):
    """Permission to manage contacts (add, change, delete)."""
    
    def has_permission(self, request, view):
        return (
            request.user.has_perm('contacts.add_contact') and
            request.user.has_perm('contacts.change_contact') and
            request.user.has_perm('contacts.delete_contact')
        )


class CanViewContactGroups(BasePermission):
    """Permission to view contact groups."""
    
    def has_permission(self, request, view):
        return request.user.has_perm('contacts.view_contactgroup')


class CanManageContactGroups(BasePermission):
    """Permission to manage contact groups."""
    
    def has_permission(self, request, view):
        return (
            request.user.has_perm('contacts.add_contactgroup') and
            request.user.has_perm('contacts.change_contactgroup') and
            request.user.has_perm('contacts.delete_contactgroup')
        )


class CanViewContactInteractions(BasePermission):
    """Permission to view contact interactions."""
    
    def has_permission(self, request, view):
        return request.user.has_perm('contacts.view_contactinteraction')


class CanAddContactInteractions(BasePermission):
    """Permission to add contact interactions."""
    
    def has_permission(self, request, view):
        return request.user.has_perm('contacts.add_contactinteraction')