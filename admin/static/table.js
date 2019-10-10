/**
 * @fileoverview This file defines controller for operating with the selected
 * table.
 */

app.controller("TableController",
  [
    '$scope', '$http', '$location', '$rootScope',
    function($scope, $http, $location, $rootScope) {
      $scope.table = $rootScope.table;
      $scope.request = {
        query: "",
      };
      $scope.data = {};

      $scope.back = function() {
        $location.path('table_list');
      };

      $scope.query = function() {
        var url = ['api', $rootScope.db.name,
                   $scope.table.name, 'query'].join('/');

        $http.post(url, $scope.request.query, {
          headers: { "Content-Type": "text/plain" }
        })
          .then(function (resp) {
            $scope.data = resp.data;
        });
      };
    }
  ]
);
