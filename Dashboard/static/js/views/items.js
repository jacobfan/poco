App.Views.Item = Backbone.View.extend({
  model: {},
  initialize: function() {
    _.bindAll(this,'render');
    this.model.bind('change',this.render);
    $('#items_nav').text('商品详情');
  },
  render: function() {
    //$(this.el).html(_.template(this.template,this.model));
    //$('#pagination').addClass('hidden');
    $('.popover').remove();
    $('.modal').remove();
    $('#main-content').html(_.template($('#item-template').html(),this.model.toJSON()));
    $('#my-modal').modal();
    return this;
  }
});

App.Views.Items = Backbone.View.extend({
  collection: {},
  initialize: function() {
    _.bindAll(this,'render');
    this.collection.bind('reset',this.render);
    $('#items_nav').text('商品列表');
  },
  render: function() {
    var v = {items: this.collection.toJSON()};
    $('#item-info').remove();
    if($('#pagination').length > 0) {
      $('#pagination ul').remove();
      $('#items-info').remove();
    }
    else
    {
      $('#main-content').append(_.template($('#pagination-template').html(), this.collection.pageInfo()));
    }
    $('#item-search-text').val($('#search_name').val());
    $('#item-search').bind('submit',function(){App.Router.navigate('page/1?s='+$('#item-search-text').val(), true);return false;});
    $('#pagination').append(_.template($('#pagination-page-template').html(), this.collection.pageInfo()));
    $('#main-content').append(_.template($('#items-template').html(),v));
    $('#items-info table tr').each(function(){
      $($(this).find("a[rel=popover]").attr('data-content')).load();
    });
    $("a[rel=popover]").popover({
      offset: 10,
      placement: 'left',
      html: true,
      fallback: '',
      delayOut: 0,
      delayIn: 10
    });
    if($('#search_name').val() != ''){
      $("#pagination ul>li>a[rel=link]").each(function(){
        var href = $(this).attr('href');
        $(this).attr('href', href+'?s='+$('#search_name').val());
      });
    }

    return this;
  },
});
