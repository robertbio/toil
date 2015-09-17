# Copyright (C) 2015 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import
from abc import ABCMeta, abstractmethod
import logging
import os
import time
import multiprocessing

from toil.common import Config
from toil.batchSystems.mesos.test import MesosTestSupport
from toil.batchSystems.parasolTestSupport import ParasolTestSupport
from toil.batchSystems.parasol import ParasolBatchSystem
from toil.batchSystems.singleMachine import SingleMachineBatchSystem
from toil.batchSystems.abstractBatchSystem import InsufficientSystemResources
from toil.test import ToilTest, needs_mesos, needs_parasol, needs_gridengine

log = logging.getLogger(__name__)

# How many cores should be utilized by this test. The test will fail if the running system doesn't have at least that
# many cores.
#
numCores = 2

# How many jobs to run. This is only read by tests that run multiple jobs.
#
numJobs = 2

# How many cores to allocate for a particular job
#
numCoresPerJob = (numCores) / numJobs


class hidden:
    """
    Hide abstract base class from unittest's test case loader

    http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class#answer-25695512
    """

    class AbstractBatchSystemTest(ToilTest):
        """
        A base test case with generic tests that every batch system should pass
        """
        __metaclass__ = ABCMeta

        @abstractmethod
        def createBatchSystem(self):
            """
            :rtype: AbstractBatchSystem
            """
            raise NotImplementedError

        def _createDummyConfig(self):
            return Config()

        def setUp(self):
            super(hidden.AbstractBatchSystemTest, self).setUp()
            self.config = self._createDummyConfig()
            self.batchSystem = self.createBatchSystem()
            self.tempDir = self._createTempDir('testFiles')

        def tearDown(self):
            self.batchSystem.shutdown()
            super(hidden.AbstractBatchSystemTest, self).tearDown()

        def testAvailableCores(self):
            self.assertTrue(multiprocessing.cpu_count() >= numCores)

        def testIssueJob(self):
            test_path = os.path.join(self.tempDir, 'test.txt')
            # sleep 1 coupled to command as 'touch' was too fast for wait_for_jobs to catch
            jobCommand = 'touch {}; sleep 1'.format(test_path)
            self.batchSystem.issueBatchJob(jobCommand, memory=10, cores=1, disk=1000)
            self.wait_for_jobs(wait_for_completion=True)
            self.assertTrue(os.path.exists(test_path))

        def testCheckResourceRequest(self):
            self.assertRaises(InsufficientSystemResources, self.batchSystem.checkResourceRequest,
                              memory=1000, cores=200, disk=1e9)
            self.assertRaises(InsufficientSystemResources, self.batchSystem.checkResourceRequest,
                              memory=5, cores=200, disk=1e9)
            self.assertRaises(InsufficientSystemResources, self.batchSystem.checkResourceRequest,
                              memory=1001e9, cores=1, disk=1e9)
            self.assertRaises(InsufficientSystemResources, self.batchSystem.checkResourceRequest,
                              memory=5, cores=1, disk=2e9)
            self.assertRaises(AssertionError, self.batchSystem.checkResourceRequest, memory=None,
                              cores=1, disk=1000)
            self.assertRaises(AssertionError, self.batchSystem.checkResourceRequest, memory=10,
                              cores=None, disk=1000)
            self.batchSystem.checkResourceRequest(memory=10, cores=1, disk=100)

        def testGetIssuedJobIDs(self):
            issuedIDs = []
            issuedIDs.append(
                self.batchSystem.issueBatchJob('sleep 1', memory=10, cores=numCoresPerJob,
                                               disk=1000))
            issuedIDs.append(
                self.batchSystem.issueBatchJob('sleep 1', memory=10, cores=numCoresPerJob,
                                               disk=1000))
            self.assertEqual(set(issuedIDs), set(self.batchSystem.getIssuedBatchJobIDs()))

        def testGetRunningJobIDs(self):
            issuedIDs = []
            issuedIDs.append(
                self.batchSystem.issueBatchJob('sleep 100', memory=100e6, cores=1, disk=1000))
            issuedIDs.append(
                self.batchSystem.issueBatchJob('sleep 100', memory=100e6, cores=1, disk=1000))
            self.wait_for_jobs(numJobs=2)
            # Assert that the issued jobs are running
            self.assertEqual(set(issuedIDs), set(self.batchSystem.getRunningBatchJobIDs().keys()))
            log.info("running jobs: %s" % self.batchSystem.getRunningBatchJobIDs().keys())
            # Assert that the length of the job was recorded
            self.assertTrue(
                len([t for t in self.batchSystem.getRunningBatchJobIDs().values() if t > 0]) == 2)
            self.batchSystem.killBatchJobs(issuedIDs)

        def testKillJobs(self):
            jobCommand = 'sleep 100'
            jobID = self.batchSystem.issueBatchJob(jobCommand, memory=100e6, cores=1, disk=1000)
            self.wait_for_jobs()
            # self.assertEqual([0], self.batchSystem.getRunningJobIDs().keys())
            self.batchSystem.killBatchJobs([jobID])
            self.assertEqual({}, self.batchSystem.getRunningBatchJobIDs())
            # Make sure that killJob doesn't hang / raise KeyError on unknown job IDs
            self.batchSystem.killBatchJobs([0])

        def testGetUpdatedJob(self):
            delay = 20
            jobCommand = 'sleep %i' % delay
            issuedIDs = []
            for i in range(numJobs):
                issuedIDs.append(
                    self.batchSystem.issueBatchJob(jobCommand, memory=100e6, cores=numCoresPerJob,
                                                   disk=1000))
            jobs = set((issuedIDs[i], 0) for i in range(numJobs))
            self.wait_for_jobs(numJobs=numJobs, wait_for_completion=True)
            for i in range(numJobs):
                jobs.remove(self.batchSystem.getUpdatedBatchJob(delay * 2))
            self.assertFalse(jobs)

        def testGetRescueJobFrequency(self):
            self.assertTrue(self.batchSystem.getRescueBatchJobFrequency() > 0)

        def wait_for_jobs(self, numJobs=1, wait_for_completion=False):
            while not self.batchSystem.getIssuedBatchJobIDs():
                pass
            while not len(self.batchSystem.getRunningBatchJobIDs().keys()) == numJobs:
                time.sleep(0.1)
            if wait_for_completion:
                while self.batchSystem.getRunningBatchJobIDs():
                    time.sleep(0.1)
                    # pass updates too quickly (~24e6 iter/sec), which is why I'm using time.sleep(0.1):


@needs_mesos
class MesosBatchSystemTest(hidden.AbstractBatchSystemTest, MesosTestSupport):
    """
    Tests against the Mesos batch system
    """

    def createBatchSystem(self):
        from toil.batchSystems.mesos.batchSystem import MesosBatchSystem
        self._startMesos(numCores)
        return MesosBatchSystem(config=self.config, maxCores=numCores, maxMemory=1e9, maxDisk=1001,
                                masterIP='127.0.0.1:5050')

    def tearDown(self):
        self._stopMesos()
        super(MesosBatchSystemTest, self).tearDown()


class SingleMachineBatchSystemTest(hidden.AbstractBatchSystemTest):
    def createBatchSystem(self):
        return SingleMachineBatchSystem(config=self.config, maxCores=numCores, maxMemory=1e9,
                                        maxDisk=1001)


@needs_parasol
class ParasolBatchSystemTest(hidden.AbstractBatchSystemTest, ParasolTestSupport):
    """
    Tests the Parasol batch system
    """
    def _createDummyConfig(self):
        config = super(ParasolBatchSystemTest, self)._createDummyConfig()
        # can't use _getTestJobStorePath since that method removes the directory
        config.jobStore = self._createTempDir('jobStore')
        return config

    def createBatchSystem(self):
        self._startParasol(numCores)
        return ParasolBatchSystem(config=self.config, maxCores=numCores, maxMemory=3e9,
                                  maxDisk=1001)

    def tearDown(self):
        self._stopParasol()
        super(ParasolBatchSystemTest, self).tearDown()


    def testIssueJob(self):
        # TODO
        # parasol treats 'touch test.txt; sleep 1' as one
        # command with the arguments 'test.txt;', 'sleep', and '1'
        # For now, override the test
        test_path = os.path.join(self.tempDir, 'test.txt')
        jobCommand = 'touch {}'.format(test_path)
        self.batchSystem.issueBatchJob(jobCommand, memory=100e6, cores=1, disk=1000)
        self.wait_for_jobs(wait_for_completion=True)
        self.assertTrue(os.path.exists(test_path))
    def testBatchResourceLimits(self):
        #self.batchSystem.issueBatchJob("sleep 100", memory=1e9, cores=1, disk=1000)
        job1 = self.batchSystem.issueBatchJob("sleep 100", memory=1e9, cores=1, disk=1000)
        job2 = self.batchSystem.issueBatchJob("sleep 100", memory=2e9, cores=1, disk=1000)

        batches = self._getBatchList()
        self.assertEqual(len(batches), 2)

        #It would be better to directly check that the batches
        #have the correct memory and cpu values, but parasol seems
        #to slightly change the values sometimes.
        self.assertTrue(batches[0]["ram"] != batches[1]["ram"])

    def _parseBatchString(self, batchString):
        import re
        batchInfo = dict()
        memPattern = re.compile("(\d+\.\d+)([kgmbt])")
        items = batchString.split()
        batchInfo["cores"] = int(items[7])
        name = str(items[11])
        memMatch = memPattern.match(items[8])
        ramValue = float(memMatch.group(1))
        ramUnits = memMatch.group(2)
        ramConversion = {'b':1e0, 'k':1e3, 'm':1e6, 'g':1e9, 't':1e12}
        batchInfo["ram"] = ramValue * ramConversion[ramUnits]
        return batchInfo

    def _getBatchList(self):
        from toil.batchSystems.parasol import popenParasolCommand
        exitStatus, batchLines = popenParasolCommand("parasol list batches")
        return [self._parseBatchString(line) for line in batchLines[1:] if not line == ""]

@needs_gridengine
class GridEngineTest(hidden.AbstractBatchSystemTest):
    """
    Tests against the GridEngine batch system
    """

    def _createDummyConfig(self):
        config = super(GridEngineTest, self)._createDummyConfig()
        # can't use _getTestJobStorePath since that method removes the directory
        config.jobStore = self._createTempDir('jobStore')
        return config

    def createBatchSystem(self):
        from toil.batchSystems.gridengine import GridengineBatchSystem
        return GridengineBatchSystem(config=self.config, maxCores=numCores, maxMemory=1000e9, maxDisk=1e9)

    @classmethod
    def setUpClass(cls):
        super(GridEngineTest, cls).setUpClass()
        logging.basicConfig(level=logging.DEBUG)
