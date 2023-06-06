from django.core.management.base import BaseCommand
from datetime import datetime

from accounts.models import Company
from data.syncClients import get_serviceTitan_invoices


class Command(BaseCommand):
    help = 'Get Revenue Values From Service Titan'

    def add_arguments(self , parser):
        parser.add_argument('-c', '--company')
    
    def handle(self, *args, **options):
        company = options['company']
        if company:
            get_serviceTitan_invoices.delay(company_id=company)
        else:
            daysToRun = [0]
            dt = datetime.now()
            weekday = dt.weekday()
            if weekday in daysToRun:
                companies = Company.objects.all()
                for company in companies:
                    if company.crm == 'ServiceTitan':
                        get_serviceTitan_invoices.delay(company.id)