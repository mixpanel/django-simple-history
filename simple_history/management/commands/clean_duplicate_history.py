from django.db import transaction
from django.utils import timezone

from ... import utils
from . import populate_history

import time

class Command(populate_history.Command):
    args = "<app.model app.model ...>"
    help = (
        "Scans HistoricalRecords for identical sequencial entries "
        "(duplicates) in a model and deletes them."
    )

    DONE_CLEANING_FOR_MODEL = "Removed {count} historical records for {model}\n"

    def add_arguments(self, parser):
        parser.add_argument("models", nargs="*", type=str)
        parser.add_argument(
            "--auto",
            action="store_true",
            dest="auto",
            default=False,
            help="Automatically search for models with the HistoricalRecords field "
            "type",
        )
        parser.add_argument(
            "-d", "--dry", action="store_true", help="Dry (test) run only, no changes"
        )
        parser.add_argument(
            "-m", "--minutes", type=int, help="Only search the last MINUTES of history"
        )
        parser.add_argument(
            "--excluded_fields",
            nargs="+",
            help="List of fields to be excluded from the diff_against check",
        )
        parser.add_argument(
            "--base-manager",
            action="store_true",
            default=False,
            help="Use Django's base manager to handle all records stored in the"
            " database, including those that would otherwise be filtered or modified"
            " by a custom manager.",
        )
        parser.add_argument(
            "--batch-size", type=int, default=None, help="Run the command in batches of batch_size, each of which will be a separate transaction",
        )
        parser.add_argument(
            "--batch-sleep", type=int, default=0, help="Number of seconds to wait in between batches",
        )

    def handle(self, *args, **options):
        self.verbosity = options["verbosity"]
        self.excluded_fields = options.get("excluded_fields")
        self.base_manager = options.get("base_manager")

        to_process = set()
        model_strings = options.get("models", []) or args

        if model_strings:
            for model_pair in self._handle_model_list(*model_strings):
                to_process.add(model_pair)

        elif options["auto"]:
            to_process = self._auto_models()

        else:
            self.log(self.COMMAND_HINT)

        self._process(to_process, date_back=options["minutes"], dry_run=options["dry"], batch_size=options["batch_size"], batch_sleep=options["batch_sleep"])

    def _process(self, to_process, date_back=None, dry_run=True, batch_size=None, batch_sleep=0):
        if date_back:
            stop_date = timezone.now() - timezone.timedelta(minutes=date_back)
        else:
            stop_date = None

        for model, history_model in to_process:
            m_qs = history_model.objects
            if stop_date:
                m_qs = m_qs.filter(history_date__gte=stop_date)
            if self.verbosity >= 2:
                found = m_qs.count()
                self.log(f"{model} has {found} historical entries", 2)
            if not m_qs.exists():
                continue

            # Break apart the query so we can add additional filtering
            if self.base_manager:
                model_query = model._base_manager.all()
            else:
                model_query = model._default_manager.all()

            # If we're provided a stop date take the initial hit of getting the
            # filtered records to iterate over
            if stop_date:
                model_query = model_query.filter(
                    pk__in=(m_qs.values_list(model._meta.pk.name).distinct())
                )

            for o in model_query.iterator():
                self._process_instance(o, model, stop_date=stop_date, dry_run=dry_run, batch_size=batch_size, batch_sleep=batch_sleep)

    def _process_instance(self, instance, model, stop_date=None, dry_run=True, batch_size=None, batch_sleep=0):
        entries_deleted = 0
        history = utils.get_history_manager_for_model(instance)
        o_qs = history.all()
        if stop_date:
            # to compare last history match
            extra_one = o_qs.filter(history_date__lte=stop_date).first()
            o_qs = o_qs.filter(history_date__gte=stop_date)
        else:
            extra_one = None

        # ordering is ('-history_date', '-history_id') so this is ok
        f1 = o_qs.first()
        if not f1:
            return

        if batch_size and batch_size > 0:
            batch_count = (len(o_qs) - 1) // batch_size
            if (len(o_qs) - 1) % batch_size > 0:
                batch_count += 1
        else:
            batch_count = 1
            batch_size = len(o_qs)

        for batch_index in range(batch_count):
            with transaction.atomic():
                batch_start = batch_size * batch_index + 1
                batch_end = min(batch_start + batch_size, len(o_qs))
                for i in range(batch_start, batch_end):
                    f2 = o_qs[i]
                    entries_deleted += self._check_and_delete(f1, f2, dry_run)
                    f1 = f2
                if extra_one:
                    entries_deleted += self._check_and_delete(f1, extra_one, dry_run)
                    extra_one = None
            if batch_index != batch_count - 1 and batch_sleep > 0:
                time.sleep(batch_sleep)

        self.log(
            self.DONE_CLEANING_FOR_MODEL.format(model=model, count=entries_deleted)
        )

    def log(self, message, verbosity_level=1):
        if self.verbosity >= verbosity_level:
            self.stdout.write(message)

    def _check_and_delete(self, entry1, entry2, dry_run=True):
        delta = entry1.diff_against(entry2, excluded_fields=self.excluded_fields)
        if not delta.changed_fields:
            if not dry_run:
                entry1.delete()
            return 1
        return 0
