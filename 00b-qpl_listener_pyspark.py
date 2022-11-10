# Databricks notebook source
from pyspark.sql.streaming import StreamingQueryListener
import logging
import sys
import json
import datetime

# COMMAND ----------

# MAGIC %run ./00a-setup

# COMMAND ----------

dbutils.fs.mkdirs(config['qps_log_directory'])

# COMMAND ----------

class MyDemoListener(StreamingQueryListener):

    def __init__(self):
      """
      Constructor for the class. Called at object creation. 
      
      Notes
      -----
      Constructor sets up a logger with Time rotating file handler. with a rollover window of 5 mins
      Only for demo purposes
      
      """
      self.logger = logging.getLogger('MyDemoListener')
      self.logger.setLevel(logging.INFO)
      logging_path = f"/dbfs/{config['qps_log_directory']}/qpl"
      fh = logging.handlers.TimedRotatingFileHandler(f'{logging_path}_demo_stream_log',when='M',interval=5, encoding=None, delay=False, errors=None, utc=True)
      fh.setLevel(logging.INFO)
      fh.suffix = "%Y_%m_%d_%H_%M_%S.txt"
      self.logger.addHandler(fh)
      pass
      
      
    def onQueryStarted(self, event):
        """
        Called when a query is started.

        Parameters
        ----------
        event: :class:`pyspark.sql.streaming.listener.QueryStartedEvent`
            The properties are available as the same as Scala API.

        Notes
        -----
        This is called synchronously with
        meth:`pyspark.sql.streaming.DataStreamWriter.start`,
        that is, ``onQueryStart`` will be called on all listeners before
        ``DataStreamWriter.start()`` returns the corresponding
        :class:`pyspark.sql.streaming.StreamingQuery`.
        Do not block in this method as it will block your query.
        """
        try: 
          msg = json.dumps({"EVENT_TIME": str(datetime.datetime.now()),"STATUS":'STREAM_STARTED','STREAM_ID': str(event.id), 'RUN_ID': str(event.runId) ,'STREAM_NAME': event.name})
          self.logger.info(msg)
        except Exception as e:
          print(f"QUERY LOGGER START FAILURE: {e}", file=sys.stderr)
        pass
      
    def onQueryProgress(self, event):
        """
        Called when there is some status update (ingestion rate updated, etc.)

        Parameters
        ----------
        event: :class:`pyspark.sql.streaming.listener.QueryProgressEvent`
            The properties are available as the same as Scala API.

        Notes
        -----
        This method is asynchronous. The status in
        :class:`pyspark.sql.streaming.StreamingQuery` will always be
        latest no matter when this method is called. Therefore, the status
        of :class:`pyspark.sql.streaming.StreamingQuery`.
        may be changed before/when you process the event.
        For example, you may find :class:`StreamingQuery`
        is terminated when you are processing `QueryProgressEvent`.
        """
        try:           
          msg = json.dumps({"EVENT_TIME":str(datetime.datetime.now()) ,"STATUS":'STREAM_UPDATED', 'BATCH_INFO': event.progress.json})          
          self.logger.info(msg)
        except Exception as e:
          print(f"QUERY LOGGER PROGRESS FAILURE: {e}",file=sys.stderr)
        
        pass

    def onQueryTerminated(self, event):
        """
        Called when a query is stopped, with or without error.

        Parameters
        ----------
        event: :class:`pyspark.sql.streaming.listener.QueryTerminatedEvent`
            The properties are available as the same as Scala API.
            
        Notes
        -----
        On query termination rollover method for the rotating file handler is triggered to generate txt file which can be read downstream.
        """
        try:      
          msg = json.dumps({"EVENT_TIME":str(datetime.datetime.now()) , "STATUS":'STREAM_TERMINATED','STREAM_ID': str(event.id), 'RUN_ID': str(event.runId) , 'TERMINATION_EXCEPTION': event.exception})          
          self.logger.info(msg)
          #rollover to generate final .txt file
          self.logger.handlers[0].doRollover()
        
        except Exception as e:
          print(f"QUERY LOGGER TERMINATED FAILUER: {e}",file=sys.stderr)
          
        pass

# COMMAND ----------

my_listener = MyDemoListener()

# COMMAND ----------

spark.streams.addListener(my_listener)
# spark.streams.removeListener(my_listener)

# COMMAND ----------

# dbutils.fs.ls(config['qps_log_directory'])
