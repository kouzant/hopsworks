'use strict';

angular.module('hopsWorksApp')
    .controller('DataValidationCtrl2', ['$scope', '$routeParams', 'ModalService', 'JobService', 'growl',
        'StorageService', '$location', 'DataValidationService',
        function ($scope, $routeParams, ModalService, JobService, growl, StorageService, $location,
            DataValidationService) {
            self = this;
            self.projectId = $routeParams.projectID;
            self.showCart = false;
            self.JOB_PREFIX = "DV-";
            self.submittingRules = false;
            self.featureGroup = {};
            self.user_rules = [];
            // Used only for UI to list existing predicates
            self.predicates = [];
            self.showCreateNewDataValidationPage = false;
            self.showValidationResult = false;
            self.validationResult = {};
            self.validationWorking = false;

            self.init = function () {
                self.featureGroup = StorageService.recover("dv_featuregroup");
                self.fetchValidationRules();
            }

            /**
             * Cart dropdown toggle
             */
            self.toggleCart = function () {
                if (self.showCart) {
                    self.showCart = false
                } else {
                    self.showCart = true
                }
            }

            self.removeRuleFromBasket = function (index) {
                self.user_rules.splice(index, 1);
            }

            // START OF RULES
            self.columnsModes = {
                NO_COLUMNS: 0,
                SINGLE_COLUMN: 1,
                MULTI_COLUMNS: 2
            }

            self.predicateType = {
                BOUNDARY: 0
            }

            var Predicate = function (name, predicateType, columnsSelectionMode, friendlyName, description) {
                this.name = name;
                this.predicateType = predicateType;
                this.friendlyName = friendlyName;
                this.description = description;
                this.constraintGroup;
                this.columnsSelectionMode = columnsSelectionMode;
                // For SIGNLE_COLUMN predicates
                this.feature;
                // For MULTI_COLUMN predicates
                this.features = [];
                this.hint = "";
            }

            Predicate.prototype.constructPredicate = function () {
                var features_names = [];
                // NO_COLUMNS
                if (this.columnsSelectionMode == 0) {
                    features_names.push('*');
                    // MULTI_COLUMNS
                } else if (this.columnsSelectionMode == 2) {
                    for (var i = 0; i < this.features.length; i++) {
                        features_names.push(this.features[i].name)
                    }
                    // SINGLE_COLUMNS
                } else if (this.columnsSelectionMode == 1) {
                    features_names.push(this.feature.name);
                }
                var args = {
                    hint: this.hint
                }
                // BOUNDARY
                if (this.predicateType == 0) {
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

                if (this.columnsSelectionMode == 2) {
                    if (this.isUndefined(this.features) || this.features.length == 0) {
                        return 3;
                    }
                }
                if (this.columnsSelectionMode == 1) {
                    if (this.isUndefined(this.feature)) {
                        return 3;
                    }
                }
                if (this.isUndefined(this.constraintGroup)) {
                    return 4;
                }
                if (this.isUndefined(this.constraintGroup)) {
                    return 5;
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
                self.columnsModes.NO_COLUMNS, "Size",
                "Assertion on the number of rows of a feature");

            var hasCompleteness = new Predicate("hasCompleteness",
                self.predicateType.BOUNDARY, self.columnsModes.MULTI_COLUMNS,
                "Completeness", "Assertion on column completeness");

            var hasUniqueness = new Predicate("hasUniqueness",
                self.predicateType.BOUNDARY, self.columnsModes.MULTI_COLUMNS,
                "Uniqueness", "Assertion on the uniqueness of a single or multiple columns");

            var hasDistinctness = new Predicate("hasDistinctness",
                self.predicateType.BOUNDARY, self.columnsModes.MULTI_COLUMNS,
                "Distinctness",
                "Assertion on the distinctness of a single or multiple columns");

            var hasUniqueValueRatio = new Predicate("hasUniqueValueRatio",
                self.predicateType.BOUNDARY, self.columnsModes.MULTI_COLUMNS,
                "Unique ratio",
                "Creates a constraint on the unique value ratio in a single or combined set of key columns");

            var hasNumberOfDistinctValues = new Predicate("hasNumberOfDistinctValues",
                self.predicateType.BOUNDARY, self.columnsModes.SINGLE_COLUMN,
                "Distinct values",
                "Assertion on the number of unique values of a column");

            var hasEntropy = new Predicate("hasEntropy",
                self.predicateType.BOUNDARY, self.columnsModes.SINGLE_COLUMN,
                "Entropy",
                "Creates a constraint that asserts on a column entropy");

            var hasMin = new Predicate("hasMin", self.predicateType.BOUNDARY,
                self.columnsModes.SINGLE_COLUMN, "Minimum",
                "Assertion on the minimum of a column");

            var hasMax = new Predicate("hasMax", self.predicateType.BOUNDARY,
                self.columnsModes.SINGLE_COLUMN, "Maximum",
                "Assertion on the maximum of a column");

            var hasMean = new Predicate("hasMean", self.predicateType.BOUNDARY,
                self.columnsModes.SINGLE_COLUMN, "Mean",
                "Assertion on the mean of a column");

            var hasSum = new Predicate("hasSum", self.predicateType.BOUNDARY,
                self.columnsModes.SINGLE_COLUMN, "Sum",
                "Assertion on the sum of the values of a column");

            var hasStandardDeviation = new Predicate("hasStandardDeviation",
                self.predicateType.BOUNDARY, self.columnsModes.SINGLE_COLUMN,
                "Standard deviation", "Assertion on the standard deviation of a column");

            self.valid_predicates = [hasSize, hasCompleteness, hasUniqueness,
                hasDistinctness, hasUniqueValueRatio, hasNumberOfDistinctValues,
                hasEntropy, hasMin, hasMax, hasMean, hasSum, hasStandardDeviation];

            // END OF RULES

            self.isUndefined = function (input) {
                return typeof input === "undefined";
            }

            // START OF VALIDATION GROUPS
            var ConstraintGroup = function (name, description, level) {
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

            var warningGroup = new ConstraintGroup('Warning', 'warning description', 'Warning');
            var errorGroup = new ConstraintGroup('Error', 'error description', 'Error');

            self.validationGroups = new Map();
            self.validationGroups.set(warningGroup, []);
            self.validationGroups.set(errorGroup, []);

            self.flatValidationGroups = [warningGroup, errorGroup];
            // END OF VALIDATION GROUPS

            self.toggleNewDataValidationPage = function () {
                self.user_rules = [];
                if (!self.showCreateNewDataValidationPage) {
                    self.showCreateNewDataValidationPage = true;
                } else {
                    self.showCreateNewDataValidationPage = false;
                }
            }

            self.returnToFeaturestore = function () {
                $location.path('project/' + self.projectId + "/featurestore");
            }

            self.fetchValidationRules = function () {
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
                            growl.error(error, { title: "Could not fetch validation rules", ttl: 2000, referenceId: "dv_growl" });
                        }
                    )
            }

            self.fetchValidationResult = function () {
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
                            growl.error(error, { title: "Could not fetch validation result", ttl: 2000, referenceId: "dv_growl" });
                            self.validationWorking = false;
                            self.showValidationResult = false;
                        }
                    )
            }

            self.addRule2DataValidation = function (rule) {
                if (self.isUndefined(rule)) {
                    console.log(">>> Rule is undefined");
                    growl.error("Rule is Undefined", { title: "Failed to add rule", ttl: 2000, referenceId: "dv_growl" });
                } else {
                    console.log(">>> Adding rule: " + rule.name);
                    var thisthis = self;
                    var features = self.featureGroup.features;
                    var newRule = new Predicate(rule.name, rule.predicateType, rule.columnsSelectionMode,
                        rule.friendlyName, rule.description);
                    ModalService.addDataValidationPredicate('lg', features, newRule, self.flatValidationGroups).then(
                        function (selectedRule) {
                            thisthis.user_rules.push(selectedRule);
                            console.log(">>> Added rule: " + selectedRule);
                            self = thisthis;
                        }, function (error) {
                            console.log(">>> OOops could not add rule");
                            self = thisthis;
                        }
                    )
                }
            }
            self.convertConstraints2DeequRules = function () {
                var groupRulesMapping = new Map();
                // First create Group -> Rules mapping
                for (var i = 0; i < self.user_rules.length; i++) {
                    var rule = self.user_rules[i];
                    var group = rule.constraintGroup;
                    var mappedGroup = groupRulesMapping.get(group);
                    if (mappedGroup) {
                        mappedGroup.push(rule);
                    } else {
                        mappedGroup = [rule];
                        groupRulesMapping.set(group, mappedGroup);
                    }
                }
                // Then convert to Deequ format
                var constraintGroups = [];
                groupRulesMapping.forEach(function (value, key) {
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

            self.convertRules2DTO = function (rules) {
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
                    groupDTO.constraints = { items: constraintsDTO };
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
                    + self.featureGroup.version + "_" + Math.round(new Date().getTime() / 1000);

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

            self.finishValidationRules = function () {
                var deequRules = self.convertConstraints2DeequRules();
                if (deequRules.constraintGroups.length == 0) {
                    growl.error("There are no Predicates", { title: "Failed creating Job", ttl: 5000, referenceId: "dv_growl" })
                    return;
                }
                var constraintsDTO = self.convertRules2DTO(deequRules);
                self.submittingRules = true;
                DataValidationService.addRules(self.projectId, self.featureGroup.featurestoreId,
                    self.featureGroup.id, constraintsDTO).then(
                        function (success) {
                            console.log("Added validation rules!")
                            var dataValidationSettings = success.data;
                            var jobConfig = self.createJobConfiguration(dataValidationSettings);
                            JobService.putJob(self.projectId, jobConfig).then(
                                function (success) {
                                    growl.info('Data Validation Job ' + jobConfig.appName + ' created',
                                        { title: 'Created Job', ttl: 5000, referenceId: "dv_growl" });
                                    self.submittingRules = false;
                                    JobService.setJobFilter(self.JOB_PREFIX + self.featureGroup.name);
                                    $location.path('project/' + self.projectId + "/jobs");
                                }, function (error) {
                                    self.submittingRules = false;
                                    var errorMsg = (typeof error.data.usrMsg !== 'undefined') ? error.data.usrMsg : "";
                                    growl.error(errorMsg, { title: 'Could not create data validation project', ttl: 5000, referenceId: "dv_growl" });
                                    self.toggleNewDataValidationPage();
                                }
                            )
                        }, function (error) {
                            self.submittingRules = false;
                            var errorMsg = (typeof error.data.usrMsg !== 'undefined') ? error.data.usrMsg : "";
                            growl.error(errorMsg, { title: 'Could not create data validation project', ttl: 5000, referenceId: "dv_growl" });
                            self.toggleNewDataValidationPage();
                        })
            }

            self.init();
        }
    ]);