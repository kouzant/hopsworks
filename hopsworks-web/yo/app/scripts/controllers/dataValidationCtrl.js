'use strict';

angular.module('hopsWorksApp')
    .controller('DataValidationCtrl', ['$scope', '$routeParams', 'ModalService', 'JobService', 'growl',
        'StorageService',
        function($scope, $routeParams, ModalService, JobService, growl, StorageService) {

            self = this

            self.showCreateNewDataValidationPage = false;
            self.projectId = $routeParams.projectID;
            self.featureGroup = {};
            // Used only for UI
            self.predicates = [];

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
              var allGroupsAreEmpty = true;
              var constraintGroups = [];
              self.constraintGroups.forEach(function(value, key) {
                if (value.length > 0) {
                  allGroupsAreEmpty = false;

                  var constraintGroup = {
                    level: key.level,
                    description: key.description
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

              if (allGroupsAreEmpty) {
                growl.error("There are no Predicates", {title: "Failed creating Job", ttl: 5000, referenceId: 1})
                return;
              }

              /* var constraints = [];
              for (var i = 0; i < self.predicates.length; i++) {
                var constraint = {
                  name: self.predicates[i].predicate,
                  hint: self.predicates[i].arguments.hint
                }
                if (self.predicates[i].arguments.min) {
                  constraint.min = self.predicates[i].arguments.min;
                }
                if (self.predicates[i].arguments.max) {
                  constraint.max = self.predicates[i].arguments.max;
                }
                constraint.columns = self.predicates[i].feature;
                constraints.push(constraint)
              }

              var constraintGroups = [];
              var constraintGroup = {
                level: "Warning",
                description: "description",
                constraints: constraints
              }
              constraintGroups.push(constraintGroup) */
              var container = {
                constraintGroups: constraintGroups
              }

              var containerJSON = JSON.stringify(container);
              console.log("Conf: " + containerJSON);
              var escaped = containerJSON.replace(/"/g, '\\"');
              escaped = escaped.replace(/,/g, '\\,');
              console.log("Conf escaped: " + escaped);
              var jobConfig = self.createJobConfiguration(escaped);
              JobService.putJob(self.projectId, jobConfig).then(
                function (success) {
                  growl.info('Data Validation Job ' + jobConfig.appName + ' created',
                      {title: 'Created Job', ttl: 5000, referenceId: 1})
                  self.toggleNewDataValidationPage();
                }, function (error) {
                  var errorMsg = (typeof error.data.usrMsg !== 'undefined')? error.data.usrMsg : "";
                  growl.error(self.errorMsg, {title: error.data.errorMsg, ttl: 5000, referenceId: 1});
                }
              )
            }

            self.createJobConfiguration = function (predicatedStr) {
              var jobName = "DataValidation-" + Math.round(new Date().getTime() / 1000);
              var executionBin = "hdfs:///user/spark/hops-verification-assembly-0.1.jar"

              var featureGroup = "--feature-group " + self.featureGroup.name;
              var featureVersion = "--feature-version " + self.featureGroup.version;
              var verificationRules = "--verification-rules \"" + predicatedStr + "\"" ;
              var cmdArgs = featureGroup + " " + featureVersion + " " + verificationRules

              var jobConfig = {};
              jobConfig.type = "sparkJobConfiguration";
              jobConfig.appName = jobName;
              jobConfig.amQueue = "default";
              jobConfig.amMemory = "2048";
              jobConfig.amVCores = "2";
              jobConfig.jobType = "SPARK";
              jobConfig.appPath = executionBin;
              jobConfig.mainClass = "io.hops.hopsworks.verification.Verification"
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