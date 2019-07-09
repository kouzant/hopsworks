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

            var ConstraintGroup = function(name, description, level) {
              this.name = name;
              this.description = description;
              this.level = level;
            }

            self.init = function () {
              self.constraintGroups = new Map();
              self.featureGroup = StorageService.recover("dv_featuregroup");
            }

            self.init();

            self.toggleNewDataValidationPage = function () {
              self.showCreateNewDataValidationPage = !self.showCreateNewDataValidationPage;
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
                    thisthis.constraintGroups.set(newGroup, []);
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
                      growl.error(self.errorMsg, {title: error.data.errorMsg, ttl: 5000, referenceId: 1});
                      self.toggleNewDataValidationPage();
                    }
                  )
                }, function (error) {
                  self.submittingRules = false;
                  var errorMsg = (typeof error.data.usrMsg !== 'undefined')? error.data.usrMsg : "";
                  growl.error(self.errorMsg, {title: error.data.errorMsg, ttl: 5000, referenceId: 1});
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
                    if (value[i].arguments.min) {
                      constraint.min = value[i].arguments.min;
                    }
                    if (value[i].arguments.max) {
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
                  if (constraints[j].min) {
                    constraintDTO.min = constraints[j].min;
                  }
                  if (constraints[j].max) {
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
        }
    ]);