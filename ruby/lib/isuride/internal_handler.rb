# frozen_string_literal: true

require 'isuride/base_handler'

module Isuride
  class InternalHandler < BaseHandler
    # このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
    # GET /api/internal/matching
    get '/matching' do
      # MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
      waiting_rides = db.query('SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at')
      if waiting_rides.size == 0
        halt 204
      end

      chairs = db.query(<<~SQL)
        select chair_id,
               longitude,
               latitude,
               speed
        from chairs
        inner join
        (select cl.chair_id,
                latitude,
                longitude
        from chair_locations
        inner join
        (select chair_id,
                max(created_at) as last_updated_at
        from chair_locations cl
        group by chair_id) cl on chair_locations.created_at = cl.last_updated_at) loc on loc.chair_id = chairs.id
        inner join chair_models cm on chairs.model = cm.name
        where is_active = true
      SQL

      waiting_rides.each do |ride|
        matched_chairs = chairs.map{|chair|
          distance = calculate_distance(ride.fetch(:pickup_latitude), ride.fetch(:pickup_longitude), chair.fetch(:latitude), chair.fetch(:longitude))
          time = distance * 1.0 / chair.fetch(:speed)
          {
            id: chair.fetch(:chair_id),
            time:,
          }
        }.sort_by{_1[:time]}
        if matched_chairs.size == 0
          halt 204
        end

        matched_chairs.each do |matched|
          empty = db.xquery('SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = ?) GROUP BY ride_id) is_completed WHERE completed = FALSE', matched.fetch(:id), as: :array).first[0]
          if empty > 0
            db.xquery('UPDATE rides SET chair_id = ? WHERE id = ?', matched.fetch(:id), ride.fetch(:id))
            break
          end
        end
      end

      204
    end
  end
end
