var app = angular
  .module('sfdb', ['ngMaterial', 'ngMessages', 'ngRoute'])
  .run(function($rootScope) {
    $rootScope.db = null
  })
  .config(function($routeProvider) {
    $routeProvider
      .when('/home', {
        templateUrl : 'home.html',
        controller : 'HomeController'
      })
      .when('/table_list', {
        templateUrl : 'table_list.html',
        controller : 'TableListController'
      })
      .when('/create_table', {
        templateUrl : 'create_table.html',
        controller : 'CreateTableController'
      })
      .when('/blog', {
        templateUrl : 'second.html',
        controller : 'SecondController'
      })
      .when('/about', {
        templateUrl : 'third.html',
        controller : 'ThirdController'
      })
      .otherwise({redirectTo: '/home'});
    });
