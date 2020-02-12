app.config(function ($stateProvider){
    $stateProvider
        .state('login', {
            url: '/login',
            templateUrl: 'modules/login/login.html'
        })
        .state('dashboard', {
            url: '/dashboard',
            templateUrl: 'modules/dashboard/dashboard.html'
        })
})
// Default Landing
.run(function ($state) {
    $state.go('login');
})