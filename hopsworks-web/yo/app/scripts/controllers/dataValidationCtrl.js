'use strict';

angular.module('hopsWorksApp')
    .controller('DataValidationCtrl', ['$scope', '$routeParams', 'ModalService', 'JobService', 'growl',
        function($scope, $routeParams, ModalService, JobService, growl) {

            self = this

            self.showCreateNewDataValidationPage = false;
            self.projectId = $routeParams.projectID;

            self.predicates = []
            self.featureGroupName = $routeParams.featureGroupName
            console.log("Feature group name: " + self.featureGroupName)

            self.toggleNewDataValidationPage = function () {
              self.showCreateNewDataValidationPage = !self.showCreateNewDataValidationPage;
            }

            self.openPredicatesEditor = function () {
                
                var feature_info = self.getFeatureGroupInfo();
                var features = feature_info['features']

                var thisthis = self
                ModalService.addDataValidationPredicate('lg', features).then(
                    function (predicate) {
                        console.log("Success from Modal " + predicate)
                        thisthis.predicates.push(predicate);
                        self = thisthis
                    }, function (error) {
                        console.log("Error from modal " + error);
                        self = thisthis
                    }
                );
            };

            self.finishPredicates = function () {
              console.log("Finished adding predicates");

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
                level: "info",
                description: "some description",
                constraints: constraints
              }
              constraintGroups.push(constraintGroup)
              var container = {
                constraintGroups: constraintGroups
              }

              var containerJSON = JSON.stringify(container);
              var escaped = containerJSON.replace(/"/g, '\\"');
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
              var executionBin = "Path to HDFS of exec file"

              var featureGroup = "--feature-group " + self.featureGroupName;
              var featureVersion = "--feature-version 1"
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
              jobConfig.mainClass = "Verification class"
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

            self.feature_group_info = {
                "type": "featuregroupDTO",
                "features": [
                  {
                    "name": "average_attendance",
                    "type": "float",
                    "description": "-",
                    "primary": false,
                    "partition": false
                  },
                  {
                    "name": "sum_attendance",
                    "type": "float",
                    "description": "-",
                    "primary": false,
                    "partition": false
                  },
                  {
                    "name": "team_id",
                    "type": "int",
                    "description": "-",
                    "primary": true,
                    "partition": false
                  }
                ],
                "featurestoreId": 12,
                "featurestoreName": "demo_featurestore_admin000_featurestore",
                "id": 141,
                "inodeId": 113084,
                "jobId": 42,
                "jobName": "featurestore_tour_job-Copy1",
                "jobStatus": "Succeeded",
                "lastComputed": "2019-07-03T16:12:55Z",
                "location": "hdfs://10.0.2.15:8020/apps/hive/warehouse/demo_featurestore_admin000_featurestore.db/attendances_features_1",
                "name": "attendances_features",
                "version": 1,
                "featuregroupType": "CACHED_FEATURE_GROUP",
                "hdfsStorePaths": [
                  "hdfs://10.0.2.15:8020/apps/hive/warehouse/demo_featurestore_admin000_featurestore.db/attendances_features_1"
                ],
                "hiveTableType": "MANAGED_TABLE",
                "inputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
              }

            self.getFeatureGroupInfo = function () {
                return self.feature_group_info;
            }
        }
    ]);