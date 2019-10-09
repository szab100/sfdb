/**
 * @fileoverview This file defines controller for listing database tables.
 */

app.controller("TableListController",
  [
    '$scope', '$http', '$location', '$rootScope',
    function($scope, $http, $location, $rootScope) {
      $scope.tables = [];

      $scope.showTables = function() {
        if (!$rootScope.db) {
          return
        }

        $http.get('/api/' + $rootScope.db.name)
          .then(function (resp) {
            $scope.tables = resp.data;
        });
      }

      $scope.createTable = function() {
        if (!$rootScope.db) {
          return
        }

        $location.path("create_table");
      }

      $scope.showTables();
    }
  ]
);