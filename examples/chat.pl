use Mojolicious::Lite;
use Mojo::SQLite;
use File::Spec::Functions 'catfile';
use File::Temp;

my $tempdir = File::Temp->newdir;
my $tempfile = catfile($tempdir, 'chat.db');

helper sqlite => sub { state $sql = Mojo::SQLite->new->from_filename($tempfile) };

get '/' => 'chat';

websocket '/channel' => sub {
  my $c = shift;

  $c->inactivity_timeout(3600);

  # Forward messages from the browser to PostgreSQL
  $c->on(message => sub { shift->sqlite->pubsub->notify(mojochat => shift) });

  # Forward messages from PostgreSQL to the browser
  my $cb = $c->sqlite->pubsub->listen(mojochat => sub { $c->send(pop) });
  $c->on(finish => sub { shift->sqlite->pubsub->unlisten(mojochat => $cb) });
};

app->start;
__DATA__

@@ chat.html.ep
<form onsubmit="sendChat(this.children[0]); return false"><input></form>
<div id="log"></div>
<script>
  var ws  = new WebSocket('<%= url_for('channel')->to_abs %>');
  ws.onmessage = function (e) {
    document.getElementById('log').innerHTML += '<p>' + e.data + '</p>';
  };
  function sendChat(input) { ws.send(input.value); input.value = '' }
</script>
