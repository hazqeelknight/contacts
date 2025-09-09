from celery import shared_task
from django.utils import timezone
from .models import Contact, ContactInteraction
import csv
import io
import logging

logger = logging.getLogger(__name__)


@shared_task
def process_contact_import(organizer_id, csv_content, skip_duplicates=True, update_existing=False):
    """Process contact import from CSV."""
    try:
        from apps.users.models import User
        organizer = User.objects.get(id=organizer_id)
        
        # Parse CSV
        csv_file = io.StringIO(csv_content)
        reader = csv.DictReader(csv_file)
        
        created_count = 0
        updated_count = 0
        skipped_count = 0
        errors = []
        
        for row in reader:
            try:
                email = row.get('email', '').strip().lower()
                if not email:
                    errors.append(f"Row {row_num}: Email is required")
                    skipped_count += 1
                    continue
                
                # Basic email validation
                if '@' not in email or '.' not in email.split('@')[-1]:
                    errors.append(f"Row {row_num}: Invalid email format: {email}")
                    skipped_count += 1
                    continue
                
                # Check if contact exists
                existing_contact = Contact.objects.filter(
                    organizer=organizer,
                    email=email
                ).first()
                
                if existing_contact:
                    if update_existing:
                        # Update existing contact
                        existing_contact.first_name = row.get('first_name', existing_contact.first_name)
                        existing_contact.last_name = row.get('last_name', existing_contact.last_name)
                        existing_contact.phone = row.get('phone', existing_contact.phone)
                        existing_contact.company = row.get('company', existing_contact.company)
                        existing_contact.job_title = row.get('job_title', existing_contact.job_title)
                        existing_contact.notes = row.get('notes', existing_contact.notes)
                        
                        # Handle tags
                        tags_str = row.get('tags', '')
                        if tags_str:
                            tags = [tag.strip() for tag in tags_str.split(',') if tag.strip()]
                            existing_contact.tags = tags
                        
                        existing_contact.save()
                        updated_count += 1
                    elif skip_duplicates:
                        skipped_count += 1
                        continue
                else:
                    # Create new contact
                    tags_str = row.get('tags', '')
                    tags = [tag.strip() for tag in tags_str.split(',') if tag.strip()] if tags_str else []
                    
                    contact = Contact.objects.create(
                        organizer=organizer,
                        first_name=row.get('first_name', ''),
                        last_name=row.get('last_name', ''),
                        email=email,
                        phone=row.get('phone', ''),
                        company=row.get('company', ''),
                        job_title=row.get('job_title', ''),
                        notes=row.get('notes', ''),
                        tags=tags
                    )
                    created_count += 1
            except Exception as e:
                logger.error(f"Error processing row {row_num}: {str(e)}")
                errors.append(f"Row {row_num}: {str(e)}")
                skipped_count += 1
        
        result = {
            'status': 'success',
            'message': f"Import completed: {created_count} created, {updated_count} updated, {skipped_count} skipped",
            'created_count': created_count,
            'updated_count': updated_count,
            'skipped_count': skipped_count,
            'errors': errors[:10]  # Limit to first 10 errors
        }
        
        if errors:
            result['status'] = 'partial_success'
            result['message'] += f" with {len(errors)} errors"
        
        return result
    
    except User.DoesNotExist:
        return {
            'status': 'error',
            'message': f"Organizer {organizer_id} not found"
        }
    except Exception as e:
        logger.error(f"Error importing contacts: {str(e)}")
        return {
            'status': 'error',
            'message': f"Error importing contacts: {str(e)}"
        }


@shared_task
def merge_contact_data(primary_contact_id, duplicate_contact_ids):
    """Merge duplicate contacts into primary contact."""
    try:
        primary_contact = Contact.objects.get(id=primary_contact_id)
        duplicate_contacts = Contact.objects.filter(id__in=duplicate_contact_ids)
        
        # Merge booking data
        from apps.events.models import Booking
        total_bookings = 0
        latest_booking_date = primary_contact.last_booking_date
        
        for duplicate in duplicate_contacts:
            # Update bookings to reference primary contact (if needed in future)
            # For now, just aggregate the statistics
            total_bookings += duplicate.total_bookings
            
            if duplicate.last_booking_date:
                if not latest_booking_date or duplicate.last_booking_date > latest_booking_date:
                    latest_booking_date = duplicate.last_booking_date
            
            # Merge interactions
            ContactInteraction.objects.filter(contact=duplicate).update(contact=primary_contact)
            
            # Merge tags
            if duplicate.tags:
                primary_tags = set(primary_contact.tags or [])
                duplicate_tags = set(duplicate.tags)
                primary_contact.tags = list(primary_tags.union(duplicate_tags))
            
            # Merge notes
            if duplicate.notes and duplicate.notes not in (primary_contact.notes or ''):
                if primary_contact.notes:
                    primary_contact.notes += f"\n\n--- Merged from {duplicate.email} ---\n{duplicate.notes}"
                else:
                    primary_contact.notes = duplicate.notes
        
        # Update primary contact
        primary_contact.total_bookings += total_bookings
        if latest_booking_date:
            primary_contact.last_booking_date = latest_booking_date
        primary_contact.save()
        
        # Delete duplicate contacts
        duplicate_contacts.delete()
        
        return {
            'status': 'success',
            'message': f"Merged {len(duplicate_contact_ids)} contacts into {primary_contact.email}",
            'merged_count': len(duplicate_contact_ids)
        }
    
    except Contact.DoesNotExist:
        return {
            'status': 'error',
            'message': f"Primary contact {primary_contact_id} not found"
        }
    except Exception as e:
        logger.error(f"Error merging contacts: {str(e)}")
        return {
            'status': 'error',
            'message': f"Error merging contacts: {str(e)}"
        }


@shared_task
def update_single_contact_booking_stats(contact_id):
    """Update booking statistics for a single contact."""
    try:
        from apps.events.models import Booking
        contact = Contact.objects.get(id=contact_id)
        
        # Get bookings for this contact
        bookings = Booking.objects.filter(
            organizer=contact.organizer,
            invitee_email=contact.email,
            status='confirmed'
        )
        
        # Update statistics
        total_bookings = bookings.count()
        last_booking = bookings.order_by('-start_time').first()
        
        contact.total_bookings = total_bookings
        contact.last_booking_date = last_booking.start_time if last_booking else None
        contact.save()
        
        return {
            'status': 'success',
            'message': f"Updated booking stats for {contact.email}",
            'total_bookings': total_bookings
        }
    
    except Contact.DoesNotExist:
        return {
            'status': 'error',
            'message': f"Contact {contact_id} not found"
        }
    except Exception as e:
        logger.error(f"Error updating contact booking stats: {str(e)}")
        return {
            'status': 'error',
            'message': f"Error updating contact booking stats: {str(e)}"
        }


@shared_task
def update_contact_booking_stats():
    """Update contact booking statistics for all contacts (periodic task)."""
    from apps.events.models import Booking
    
    # Get contacts that might need updates (more efficient than all contacts)
    contacts = Contact.objects.select_related('organizer')
    updated_count = 0
    
    for contact in contacts:
        # Get bookings for this contact
        bookings = Booking.objects.filter(
            organizer=contact.organizer,
            invitee_email=contact.email,
            status='confirmed'
        )
        
        # Update statistics
        total_bookings = bookings.count()
        last_booking = bookings.order_by('-start_time').first()
        
        if contact.total_bookings != total_bookings or (
            last_booking and contact.last_booking_date != last_booking.start_time
        ):
            contact.total_bookings = total_bookings
            contact.last_booking_date = last_booking.start_time if last_booking else None
            contact.save()
            updated_count += 1
    
    return {
        'status': 'success',
        'message': f"Updated booking stats for {updated_count} contacts",
        'updated_count': updated_count
    }


@shared_task
def create_contact_from_booking(booking_id):
    """Create or update contact from booking."""
    try:
        from apps.events.models import Booking
        booking = Booking.objects.get(id=booking_id)
        
        # Improved name parsing
        first_name = ''
        last_name = ''
        if booking.invitee_name:
            name_parts = booking.invitee_name.strip().split(' ', 1)
            first_name = name_parts[0]
            last_name = name_parts[1] if len(name_parts) > 1 else ''
        
        # Check if contact already exists
        contact, created = Contact.objects.get_or_create(
            organizer=booking.organizer,
            email=booking.invitee_email,
            defaults={
                'first_name': first_name,
                'last_name': last_name,
                'phone': booking.invitee_phone,
            }
        )
        
        # Update booking statistics
        contact.total_bookings = Booking.objects.filter(
            organizer=booking.organizer,
            invitee_email=booking.invitee_email,
            status='confirmed'
        ).count()
        
        contact.last_booking_date = booking.start_time
        contact.save()
        
        # Create interaction record
        ContactInteraction.objects.create(
            contact=contact,
            organizer=booking.organizer,
            interaction_type='booking_created',
            description=f"Booked {booking.event_type.name} for {booking.start_time.strftime('%B %d, %Y at %I:%M %p')}",
            booking=booking,
            metadata={
                'event_type': booking.event_type.name,
                'duration': booking.event_type.duration,
                'start_time': booking.start_time.isoformat()
            }
        )
        
        action = "Created" if created else "Updated"
        return {
            'status': 'success',
            'message': f"{action} contact for {booking.invitee_email}",
            'created': created
        }
    
    except Booking.DoesNotExist:
        return {
            'status': 'error',
            'message': f"Booking {booking_id} not found"
        }
    except Exception as e:
        logger.error(f"Error creating contact from booking: {str(e)}")
        return {
            'status': 'error',
            'message': f"Error creating contact from booking: {str(e)}"
        }