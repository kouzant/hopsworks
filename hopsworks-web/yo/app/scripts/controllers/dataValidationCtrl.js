'use strict';

angular.module('hopsWorksApp')
    .controller('DataValidationCtrl', ['$scope', '$routeParams', 'ModalService', 'JobService', 'growl',
        'StorageService',
        function($scope, $routeParams, ModalService, JobService, growl, StorageService) {

            self = this

            self.showCreateNewDataValidationPage = false;
            self.projectId = $routeParams.projectID;
            self.featureGroup = {};
            self.predicates = [];

            self.init = function () {
              self.featureGroup = StorageService.recover("dv_featuregroup");
            }

            self.init();

            self.toggleNewDataValidationPage = function () {
              self.showCreateNewDataValidationPage = !self.showCreateNewDataValidationPage;
            }

            self.openPredicatesEditor = function () {
              var features = self.featureGroup.features;

              var thisthis = self
              ModalService.addDataValidationPredicate('lg', features).then(
                function (predicate) {
                  thisthis.predicates.push(predicate);
                  self = thisthis
                }, function (error) {
                  self = thisthis
                }
              );
            };

            self.finishPredicates = function () {
              if (self.predicates.length == 0) {
                growl.error("There are no Predicates", {title: "Failed creating Job", ttl: 5000, referenceId: 1})
                return;
              }
              var constraints = [];
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
              constraintGroups.push(constraintGroup)
              var container = {
                constraintGroups: constraintGroups
              }

              var containerJSON = JSON.stringify(container);
              var escaped = containerJSON.replace(/"/g, '\\"');
              escaped = escaped.replace(/,/g, '\\,');
              console.log("cg: " + escaped);
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
              var executionBin = "hdfs:///user/spark/hops-verification-assembly-1.0.jar"

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
              jobConfig.mainClass = "Verification"
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