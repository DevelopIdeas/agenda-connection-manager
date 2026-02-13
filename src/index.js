const moment = require('moment')
class AgendaConntionManager {
  constructor() {
    this.collection = null
    this.connected = false
    this.connectionError = null
    this.connectionErrorAt = null
    this.disconnectTimeout = null
    this.agenda = null
    this.job_times = {}
  }

  setCollection(collection) {
    if (!collection) { return; }

    const self = this
    self.collection = collection

    let topology = self.collection.s.topology
    if (!topology) {
      topology = collection.s.db.s.client.topology
    }
    // topology.on('topologyOpening', function () {
    //   console.log('topologyOpening');
    // });
    // topology.on('topologyClosed', function () {
    //   console.log('topologyClosed');
    // });
    topology.on('topologyDescriptionChanged', function (e) {
      const { servers } = e.newDescription
      const key = servers.keys().next().value
      const server = servers.get(key)
      const { address, error } = server
      if (address) {
        if (error) {
          self.setConnected(false, error)
        } else {
          self.setConnected(true)
        }
      }
    });
    // topology.on('serverHeartbeatStarted', function () {
    //   console.log('serverHeartbeatStarted');
    // });
    // topology.on('serverHeartbeatSucceeded', function () {
    //   console.log('serverHeartbeatSucceeded');
    // });
    // topology.on('serverHeartbeatFailed', function () {
    //   // works
    //   console.log('serverHeartbeatFailed');
    // });
  }

  async setConnected(connected, error) {
    const wasConnected = this.connected
    const hadError = !!this.connectionError
    
    this.connected = connected
    this.connectionError = error
    if (error) {
      this.connectionErrorAt = new Date()
    }

    if (!connected && error) {
      __logger.error('Connection Error', { error })
      
      // Stop Agenda when connection is lost
      if (this.agenda && wasConnected) {
        try {
          __logger.warn('Stopping Agenda due to connection loss')
          await this.agenda.stop()
        } catch (ex) {
          __logger.error('Error stopping Agenda', ex)
        }
      }
      
      if (!this.disconnectTimeout) {
        // Reduced timeout from 10 minutes to 2 minutes
        this.disconnectTimeout = setTimeout(() => {
          __logger.error('Closing Process for Restart', {  error })
          process.exit(1)
        }, this.collection ? 2*60*1000 : 10*1000)
      }
    } else if (connected) {
      __logger.info('Connection Established')
      if (this.disconnectTimeout) {
        clearTimeout(this.disconnectTimeout)
        this.disconnectTimeout = null
      }
      
      // Restart Agenda when connection is restored after an error
      if (this.agenda && !wasConnected && hadError) {
        try {
          __logger.info('Restarting Agenda after connection restored')
          await this.agenda.start()
          __logger.info('Agenda restarted successfully')
        } catch (ex) {
          __logger.error('Error restarting Agenda', ex)
        }
      }
    }
  }

  setAgenda(agenda) {
    this.agenda = agenda
  }

  addJobListeners() {
    const self = this
    self.agenda.on("start", async (job) => {
      self.job_times[job.attrs.name] = moment()      
      __logger.info(`Job starting`, { job: job.attrs.name, _jobQueue: self.agenda._jobQueue.length });

      await self.updateJobData(job, { last_start: moment().toDate() })
    });
    self.agenda.on("complete", async (job) => {
      const elapsed = moment().diff(self.job_times[job.attrs.name], 'seconds')
      __logger.info(`Job complete`, {
        job: job.attrs.name,
        elapsed_sec: elapsed,
        elapsed_min: moment().diff(self.job_times[job.attrs.name], 'minutes'),
      });

      await self.updateJobData(job, { last_complete: moment().toDate(), last_elapsed: elapsed })
      // await job.save()
    });
    self.agenda.on("success", async (job) => {
      __logger.info(`Job success`, { job: job.attrs.name });

      await self.updateJobData(job, { last_success: moment().toDate() })
    });
    self.agenda.on("fail", async (err, job) => {
      // console.log(`Job ${job.attrs.name} failed with error: ${err.message}`);
      __logger.error(`Job error`, {
        job: job.attrs.name,
        message: err.message,
        stack: err.stack
      });
      await self.updateJobData(job, { last_fail: moment().toDate() })
    });
  }

  async unlockJobs() {
    const jobs = await this.agenda.jobs()

    const locked = jobs.filter(j => j.attrs.lockedAt)

    for (let job of locked) {
      __logger.debug('Unlocking Job', {
        job: job.attrs.name
      })
      job.attrs.lockedAt = null;
      await job.save();
    }
  }

  async updateJobData(job, ret, merge) {
    merge = merge === undefined ? true : merge
    try {
      if (merge) {
        ret = {...(job.attrs.data||{}), ...(ret||{})}
      } else {
        ret = ret||{}
      }
      const resp = await this.collection.findOneAndUpdate(
        {_id: job.attrs._id },
        { $set: { data: ret } },
        {returnOriginal: false}
      );
    } catch (ex) {
      __logger.error('Error updating job data', ex)
    }
  }
}

module.exports = AgendaConntionManager
