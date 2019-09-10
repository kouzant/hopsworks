/*
 * This file is part of Hopsworks
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */
'use strict';

angular.module('hopsWorksApp')
    .controller('DataValidationCtrl', ['$scope', '$routeParams', 'ModalService', 'JobService', 'growl',
        'StorageService', '$location', 'DataValidationService',
        function($scope, $routeParams, ModalService, JobService, growl, StorageService, $location,
          DataValidationService) {

            self = this
            self.JOB_PREFIX = "DV-";
            self.showCreateNewDataValidationPage = false;
            self.projectId = $routeParams.projectID;
            self.featureGroup = {};
            // Used only for UI to list predicates
            self.predicates = [];
            self.submittingRules = false;

            self.showValidationResult = false;
            self.validationResult = {};
            self.validationWorking = false;

            // START OF RULES
            self.constraintGroupLevels = ['Warning', 'Error'];

            self.columnsModes = {
                NO_COLUMNS: 0,
                SINGLE_COLUMN: 1,
                MULTI_COLUMNS: 2
            }

            self.predicateType = {
                BOUNDARY: 0
            }
            
            var Predicate = function(name, predicateType, columnsSelectionMode, validationFunction) {
                this.name = name;
                this.predicateType = predicateType;
                this.constraintGroup = {};
                this.columnsSelectionMode = columnsSelectionMode;
                // For SIGNLE_COLUMN predicates
                this.feature;
                // For MULTI_COLUMN predicates
                this.features = [];
                this.hint = "";
            }

            Predicate.prototype.constructPredicate = function () {
                var features_names = [];
                if (this.columnsSelectionMode == self.columnsModes.NO_COLUMNS) {
                    features_names.push('*');
                } else if (this.columnsSelectionMode == self.columnsModes.MULTI_COLUMNS) {
                    for (var i = 0; i < this.features.length; i++) {
                        features_names.push(this.features[i].name)
                    }
                } else if (this.columnsSelectionMode == self.columnsModes.SINGLE_COLUMN) {
                    features_names.push(this.feature.name);
                }
                var args = {
                    hint: this.hint
                }
                if (this.predicateType == self.predicateType.BOUNDARY) {
                    args.min = this.min;
                    args.max = this.max;
                }

                var predicate = {
                    feature: features_names,
                    predicate: this.name,
                    arguments: args,
                    constraintGroup: this.constraintGroup
                }
                return predicate;
            };

            Predicate.prototype.checkInput = function () {
                if (this.isUndefined(this.min) || this.isUndefined(this.max)) {
                    return 1;
                }
                if (this.isUndefined(this.hint) || this.hint.length == 0) {
                    return 2;
                }

                if (this.columnsSelectionMode == self.columnsModes.MULTI_COLUMNS) {
                    if (this.isUndefined(this.features) || this.features.length == 0) {
                        return 3;
                    }
                }
                if (this.columnsSelectionMode == self.columnsModes.SINGLE_COLUMN) {
                    if (this.isUndefined(this.feature)) {
                        return 3;
                    }
                }
                if (this.isUndefined(this.constraintGroup)) {
                    return 4;
                }
                return -1;
            }

            Predicate.prototype.isUndefined = function (input) {
                return typeof input === "undefined";
            }

            /*
            ** Deequ rules
            */
            var hasSize = new Predicate("hasSize", self.predicateType.BOUNDARY,
                self.columnsModes.NO_COLUMNS);
            var hasCompleteness = new Predicate("hasCompleteness",
                self.predicateType.BOUNDARY, self.columnsModes.MULTI_COLUMNS);
            var hasUniqueness = new Predicate("hasUniqueness",
                self.predicateType.BOUNDARY, self.columnsModes.MULTI_COLUMNS);
            var hasDistinctness = new Predicate("hasDistinctness",
                self.predicateType.BOUNDARY, self.columnsModes.MULTI_COLUMNS);
            var hasUniqueValueRatio = new Predicate("hasUniqueValueRatio",
                self.predicateType.BOUNDARY, self.columnsModes.MULTI_COLUMNS);
            var hasNumberOfDistinctValues = new Predicate("hasNumberOfDistinctValues",
                self.predicateType.BOUNDARY, self.columnsModes.SINGLE_COLUMN);
            var hasEntropy = new Predicate("hasEntropy",
                self.predicateType.BOUNDARY, self.columnsModes.SINGLE_COLUMN);
            var hasMin = new Predicate("hasMin", self.predicateType.BOUNDARY,
                self.columnsModes.SINGLE_COLUMN);
            var hasMax = new Predicate("hasMax", self.predicateType.BOUNDARY,
                self.columnsModes.SINGLE_COLUMN);
            var hasMean = new Predicate("hasMean", self.predicateType.BOUNDARY,
                self.columnsModes.SINGLE_COLUMN);
            var hasSum = new Predicate("hasSum", self.predicateType.BOUNDARY,
                self.columnsModes.SINGLE_COLUMN);
            var hasStandardDeviation = new Predicate("hasStandardDeviation",
                self.predicateType.BOUNDARY, self.columnsModes.SINGLE_COLUMN);

            self.valid_predicates = [hasSize, hasCompleteness, hasUniqueness,
                hasDistinctness, hasUniqueValueRatio, hasNumberOfDistinctValues,
                hasEntropy, hasMin, hasMax, hasMean, hasSum, hasStandardDeviation];
              
            // END OF RULES

            var ConstraintGroup = function(name, description, level) {
              this.name = name;
              this.description = description;
              this.level = level;
            }

            ConstraintGroup.prototype.checkInput = function () {
              if (this.isUndefined(this.name) || this.name.length == 0) {
                return 1;
              }
              if (this.isUndefined(this.description) || this.description.length == 0) {
                return 2;
              }
              if (this.isUndefined(this.level) || this.level.length == 0) {
                return 3;
              }
              return -1;
          }

          ConstraintGroup.prototype.isUndefined = function (input) {
            return typeof input === "undefined";
          }

            self.init = function () {
              self.constraintGroups = new Map();
              self.featureGroup = StorageService.recover("dv_featuregroup");
              self.fetchValidationRules();
            }

            self.fetchValidationRules = function() {
              self.validationWorking = true;
              DataValidationService.getRules(self.projectId, self.featureGroup.featurestoreId,
                self.featureGroup.id).then(
                  function (success) {
                    self.predicates = [];
                    self.convertDTO2Rules(success.data);
                    self.showValidationResult = false;
                    self.validationResult = {};
                    self.validationWorking = false;
                  }, function (error) {
                    self.validationWorking = false;
                    growl.error(error, {title: "Could not fetch validation rules", ttl: 2000, referenceId: 1});
                  }
                )
            }

            self.fetchValidationResult = function() {
              self.validationWorking = true;
              DataValidationService.getResult(self.projectId, self.featureGroup.featurestoreId,
                self.featureGroup.id).then(
                  function (success) {
                    self.validationResult.status = success.data.status.toUpperCase();
                    if (self.validationResult.status !== 'EMPTY') {
                      self.validationResult.constraintsResult = success.data.constraintsResult;
                    }
                    self.validationWorking = false;
                    self.showValidationResult = true;
                  }, function (error) {
                    growl.error(error, {title: "Could not fetch validation result", ttl: 2000, referenceId: 1});
                    self.validationWorking = false;
                    self.showValidationResult = false;
                  }
                )
            }

            self.toggleNewDataValidationPage = function () {
              self.predicates = [];
              self.constraintGroups = new Map();
              if (!self.showCreateNewDataValidationPage) {
                self.predicates = [];
                self.constraintGroups = new Map();
                self.showCreateNewDataValidationPage = true;
              } else {
                self.fetchValidationRules();
                self.showCreateNewDataValidationPage = false;
              }
            }

            self.hasConstraintGroups = function () {
              return self.constraintGroups.size > 0;
            }

            self.openPredicatesEditor = function () {
              var features = self.featureGroup.features;

              var thisthis = self
              var constraintGroupsFlat = [];
              self.constraintGroups.forEach(function(value, key) {
                constraintGroupsFlat.push(key);
              })
              ModalService.addDataValidationPredicate('lg', features, constraintGroupsFlat).then(
                function (predicate) {
                  var groupPredicates = thisthis.constraintGroups.get(predicate.constraintGroup);
                  if (groupPredicates) {
                    groupPredicates.push(predicate);
                  }
                  thisthis.predicates.push(predicate);
                  self = thisthis
                }, function (error) {
                  self = thisthis
                }
              );
            };

            self.openConstraintGroupsPage = function () {
              var thisthis = self;
              ModalService.createConstraintGroup('lg').then(
                function (group) {
                  if (group) {
                    var newGroup = new ConstraintGroup(group.name, group.description, group.level);
                    if (newGroup.checkInput() > 0) {
                      growl.error("Group missing required arguments", {title: "Did not create constraint group", ttl: 2000, referenceId: 1});
                    } else {
                      thisthis.constraintGroups.set(newGroup, []);
                    }
                  } else {
                    growl.error("Constraint group is empty", {title: "Failed creating constraint group", ttl: 2000, referenceId: 1});
                  }
                  self = thisthis;
                }, function (error) {
                  growl.error(error, {title: "Failed creating constraint group", ttl: 2000, referenceId: 1});
                  self = thisthis;
                }
              )
            }

            self.finishPredicates = function () {
              var deequRules = self.convertConstraints2DeequRules();
              if (deequRules.constraintGroups.length == 0) {
                growl.error("There are no Predicates", {title: "Failed creating Job", ttl: 5000, referenceId: 1})
                return;
              }

              var constraintsDTO = self.convertRules2DTO(deequRules);
              console.log("Rules DTO: " + JSON.stringify(constraintsDTO));
              self.submittingRules = true;
              DataValidationService.addRules(self.projectId, self.featureGroup.featurestoreId,
                self.featureGroup.id, constraintsDTO)
                .then(function (success) {
                  console.log("Added validation rules!")
                  var dataValidationSettings = success.data;
                  var jobConfig = self.createJobConfiguration(dataValidationSettings);
                  JobService.putJob(self.projectId, jobConfig).then(
                    function (success) {
                      growl.info('Data Validation Job ' + jobConfig.appName + ' created',
                        {title: 'Created Job', ttl: 5000, referenceId: 1});
                      self.submittingRules = false;
                      JobService.setJobFilter(self.JOB_PREFIX + self.featureGroup.name);
                      $location.path('project/' + self.projectId + "/jobs");
                    }, function (error) {
                      self.submittingRules = false;
                      var errorMsg = (typeof error.data.usrMsg !== 'undefined')? error.data.usrMsg : "";
                      growl.error(errorMsg, {title: 'Could not create data validation project', ttl: 5000, referenceId: 1});
                      self.toggleNewDataValidationPage();
                    }
                  )
                }, function (error) {
                  self.submittingRules = false;
                  var errorMsg = (typeof error.data.usrMsg !== 'undefined')? error.data.usrMsg : "";
                  growl.error(errorMsg, {title: 'Could not create data validation project', ttl: 5000, referenceId: 1});
                  self.toggleNewDataValidationPage();
                })
            }

            self.convertConstraints2DeequRules = function () {
              var constraintGroups = [];
              self.constraintGroups.forEach(function (value, key) {
                if (value.length > 0) {
                  var constraintGroup = {
                    level: key.level,
                    description: key.description,
                    name: key.name
                  }
                  var constraints = [];
                  for (var i = 0; i < value.length; i++) {
                    var constraint = {
                      name: value[i].predicate,
                      hint: value[i].arguments.hint
                    }
                    if (!self.isUndefined(value[i].arguments.min)) {
                      constraint.min = value[i].arguments.min;
                    }
                    if (!self.isUndefined(value[i].arguments.max)) {
                      constraint.max = value[i].arguments.max;
                    }
                    constraint.columns = value[i].feature;
                    constraints.push(constraint)
                  }
                  constraintGroup.constraints = constraints;
                  constraintGroups.push(constraintGroup);
                }
              })

              var container = {
                constraintGroups: constraintGroups
              }
              return container;
            }

            self.isUndefined = function (input) {
              return typeof input === "undefined";
            }

            self.convertRules2DTO = function(rules) {
              var groupsContainer = {};
              groupsContainer.type = "constraintGroupDTO";

              var groups = rules.constraintGroups;
              var groupsDTO = [];
              for (var i = 0; i < groups.length; i++) {
                var group = groups[i];
                var groupDTO = {};
                groupDTO.type = "constraintGroupDTO";
                groupDTO.name = group.name;
                groupDTO.description = group.description;
                groupDTO.level = group.level;
                var constraints = group.constraints;
                var constraintsDTO = [];
                for (var j = 0; j < constraints.length; j++) {
                  var constraintDTO = {};
                  constraintDTO.type = "constraintDTO";
                  constraintDTO.name = constraints[j].name;
                  constraintDTO.hint = constraints[j].hint;
                  constraintDTO.columns = constraints[j].columns;
                  if (!self.isUndefined(constraints[j].min)) {
                    constraintDTO.min = constraints[j].min;
                  }
                  if (!self.isUndefined(constraints[j].max)) {
                    constraintDTO.max = constraints[j].max;
                  }
                  constraintsDTO.push(constraintDTO);
                }
                groupDTO.constraints = {items: constraintsDTO};
                groupsDTO.push(groupDTO);
              }
              groupsContainer.items = groupsDTO;
              return groupsContainer;
            }

            /*
             * Used to convert existing rules to flat predicates and print them
            */
            self.convertDTO2Rules = function (dto) {
              var constraintGroups = dto.items;
              if (!constraintGroups) {
                return;
              }
              for (var i = 0; i < constraintGroups.length; i++) {
                var constraintGroupJ = constraintGroups[i];
                var constraintGroup = new ConstraintGroup(constraintGroupJ.name, constraintGroupJ.description,
                  constraintGroupJ.level);
                var constraintsJ = constraintGroupJ.constraints.items;
                for (var j = 0; j < constraintsJ.length; j++) {
                  var constraintJ = constraintsJ[j];
                  var constraint = {};
                  constraint.predicate = constraintJ.name;
                  constraint.feature = constraintJ.columns;
                  constraint.constraintGroup = constraintGroup;
                  constraint.arguments = "";
                  if (!self.isUndefined(constraintJ.min)) {
                    constraint.arguments += "min: " + constraintJ.min;
                  }
                  if (!self.isUndefined(constraintJ.max)) {
                    constraint.arguments += " max: " + constraintJ.max;
                  }
                  self.predicates.push(constraint);
                }
              }
            }

            self.createJobConfiguration = function (dataValidationSettings) {
              var jobName = self.JOB_PREFIX + self.featureGroup.name + "-v"
                + self.featureGroup.version +"_"+ Math.round(new Date().getTime() / 1000);

              var featureGroup = "--feature-group " + self.featureGroup.name;
              var featureVersion = "--feature-version " + self.featureGroup.version;
              var verificationRulesPath = "--verification-rules-file " + dataValidationSettings.validationRulesPath;

              var cmdArgs = featureGroup + " " + featureVersion + " " + verificationRulesPath;

              var jobConfig = {};
              jobConfig.type = "sparkJobConfiguration";
              jobConfig.appName = jobName;
              jobConfig.amQueue = "default";
              jobConfig.amMemory = "2048";
              jobConfig.amVCores = "2";
              jobConfig.jobType = "SPARK";
              jobConfig.appPath = dataValidationSettings.executablePath;
              jobConfig.mainClass = dataValidationSettings.executableMainClass;
              jobConfig.args = cmdArgs;
              jobConfig['spark.blacklist.enabled'] = false;
              jobConfig['spark.dynamicAllocation.enabled'] = true;
              jobConfig['spark.dynamicAllocation.minExecutors'] = 2;
              jobConfig['spark.dynamicAllocation.maxExecutors'] = 20;
              jobConfig['spark.dynamicAllocation.initialExecutors'] = 3;
              jobConfig['spark.executor.memory'] = "2048";
              jobConfig['spark.executor.cores'] = 2;

              return jobConfig;
            }

            self.init();
        }
    ]);