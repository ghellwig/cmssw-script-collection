import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing

options = VarParsing ("analysis")
options.parseArguments()

process = cms.Process("Copy")

process.load("FWCore.MessageService.MessageLogger_cfi")
process.MessageLogger.cerr.FwkReport.reportEvery = 1000

process.source = cms.Source("PoolSource",
                            fileNames = cms.untracked.vstring(options.inputFiles))

process.o = cms.OutputModule("PoolOutputModule",
                             fileName = cms.untracked.string(options.outputFile),
                             fastCloning = cms.untracked.bool(False),
                             overrideInputFileSplitLevels = cms.untracked.bool(True),
                             splitLevel = cms.untracked.int32(0))

process.out = cms.EndPath(process.o)
process.dumpPython()
