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

  setConnected(connected, error) {
    this.connected = connected
    this.connectionError = error
    if (error) {
      this.connectionErrorAt = new Date()
    }

    if (!connected && error) {
      __logger.error('Connection Error', { error })
      if (!this.disconnectTimeout) {
        this.disconnectTimeout = setTimeout(() => {
          __logger.error('Closing Process for Restart', {  error })
          process.exit(1)
        }, this.collection ? 10*60*1000 : 10*1000)
      }
    } else if (connected) {
      __logger.info('Connection Established')
      if (this.disconnectTimeout) {
        clearTimeout(this.disconnectTimeout)
        this.disconnectTimeout = null
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

      job.attrs.data.last_start = self.job_times[job.attrs.name].toDate()
      await job.save()
    });
    self.agenda.on("complete", async (job) => {
      const elapsed = moment().diff(self.job_times[job.attrs.name], 'seconds')
      __logger.info(`Job complete`, {
        job: job.attrs.name,
        elapsed_sec: elapsed,
        elapsed_min: moment().diff(self.job_times[job.attrs.name], 'minutes'),
      });

      job.attrs.data.last_complete = moment().toDate()
      job.attrs.data.last_elapsed = elapsed
      await job.save()
    });
    self.agenda.on("success", async (job) => {
      __logger.info(`Job success`, { job: job.attrs.name });

      job.attrs.data.last_success = moment().toDate()
      await job.save()
    });
    self.agenda.on("fail", async (err, job) => {
      // console.log(`Job ${job.attrs.name} failed with error: ${err.message}`);
      __logger.error(`Job error`, {
        job: job.attrs.name,
        message: err.message,
        stack: err.stack
      });

      job.attrs.data.last_fail = moment().toDate()
      await job.save()
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
        job.attrs.data = {...(job.attrs.data||{}), ...(ret||{})}
      } else {
        job.attrs.data = ret||{}
      }
      await job.save()
    } catch (ex) {
      __logger.error('Error updating job data', ex)
    }
  }
}

module.exports = AgendaConntionManager