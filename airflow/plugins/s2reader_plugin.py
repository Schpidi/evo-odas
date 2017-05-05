import logging

from airflow.operators import BashOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class S2ReaderEOOMOperator(BashOperator):

    @apply_defaults
    def __init__(self, input_safe_package, output_directory, *args, **kwargs):
        self.input_safe_package = input_safe_package
        self.output_directory = output_directory
        self.s2_transform_command = 'for resolution in 10 20 60 ; do s2_transform --resolution ${resolution} --out-file "%s/$(basename "%s")__${resolution}m.xml" --single-granule %s; done' % (self.output_directory, self.input_safe_package, self.input_safe_package)
        super(S2ReaderEOOMOperator, self).__init__(bash_command=self.s2_transform_command, *args, **kwargs)

    def execute(self, context):
        log.info("s2reader generate EO-O&M")
        log.info("Input SAFE package: %s", self.input_safe_package)
        log.info("Output filename: %s", self.output_directory)
        log.info("The full command is: %s", self.s2_transform_command)
        BashOperator.execute(self, context)


class S2ReaderPlugin(AirflowPlugin):
    name = "s2reader_plugin"
    operators = [S2ReaderEOOMOperator, ]
