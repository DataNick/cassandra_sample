require 'cql'

class SampleApp
  def connect(host)
    @cluster = Cql::Client.connect(host: host)
  end

  def close
    @cluster.close
  end

  def create_schema
    create_keyspace = <<-CQL
      CREATE KEYSPACE IF NOT EXISTS portfolio_demo
        WITH REPLICATION = { 'class': 'SimpleStrategy',
                              'replication_factor': 1 };
    CQL

    create_portfolio = <<-CQL
      CREATE TABLE IF NOT EXISTS portfolio_demo.portfolio (
      portfolio_id UUID,
      ticker TEXT,
      current_price DECIMAL,
      current_change DECIMAL,
      current_change_percent FLOAT,
      PRIMARY KEY (portfolio_id, ticker)
      );
    CQL

    @cluster.execute(create_keyspace)
    @cluster.execute(create_portfolio)
  end

  def load_data
    row_one = <<-CQL
      INSERT INTO portfolio_demo.portfolio
        (portfolio_id, ticker, current_price,
        current_change, current_change_percent)

      VALUES
        (756716f7-2e54-4715-9f00-91dcbea6cf50,
          'GOOG', 889.07, -4.00, -0.45);
    CQL

    row_two = <<-CQL
      INSERT INTO portfolio_demo.portfolio
        (portfolio_id, ticker, current_price,
        current_change, current_change_percent)

      VALUES
        (756716f7-2e54-4715-9f00-91dcbea6cf50,
          'AMZN', 297.92, -0.94, -0.31);
    CQL

    @cluster.execute(row_one)
    @cluster.execute(row_two)
  end

  def print_results
    fields = %w(ticker current_price current_change current_change_percent)
    results_query = <<-CQL
      SELECT * FROM portfolio_demo.portfolio
        WHERE portfolio_id = 756716f7-2e54-4715-9f00-91dcbea6cf50;
    CQL

    puts "Ticker\tPrice\tChange\tPCT"
    puts '.........+..........+.........+.........'

    results = @cluster.execute(results_query)
    results.each do |row|
      puts "%s\t%0.2f\t%0.2f\t%0.2f" % fields.map{|f| row[f]}
    end
  end

end

if __FILE__ == $0
  sample_app = SampleApp.new
  sample_app.connect('127.0.0.1')
  sample_app.create_schema
  sample_app.load_data
  sample_app.print_results
  sample_app.close
end