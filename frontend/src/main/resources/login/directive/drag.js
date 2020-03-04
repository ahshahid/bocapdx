app.directive('dragMe', dragMe)
  app.directive('dropOnMe', dropOnMe);

dragMe.$inject = [];

function dragMe() {
  var DDO1 = {
    restrict: 'A',
    link: function(scope, element, attrs) {
      element.prop('draggable', true);
      element.on('dragstart', function(event) {
        event.dataTransfer.setData('text', event.target.id)
      });
    }
  };
  return DDO1;
}
dropOnMe.$inject = [];
function dropOnMe() {
  var DDO = {
    restrict: 'A',
    link: function(scope, element, attrs) {
      element.on('dragover', function(event) {
        event.preventDefault();
      });
      element.on('drop', function(event) {
        event.preventDefault();
        var data = event.dataTransfer.getData("text");
        //event.target.appendChild(document.getElementById(data));
      });
    }
  };
  return DDO;
}
