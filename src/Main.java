
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;


public class Main {

    public static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306/";
    public static final String TABLE_NAME = "minions_db";
    public static final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    private static Connection connection;

    public static void main(String[] args) throws SQLException, IOException {

        connection = getConnection();

        System.out.println("Enter exercise number:");
        int exNum = Integer.parseInt(reader.readLine());

        switch (exNum) {
            case 2 -> exTwo();
            case 3 -> exThree();
            case 4 -> exFour();
            case 5 -> exFive();
            case 6 -> exSix();
            case 7 -> exSeven();
            case 8 -> exEight();
            case 9 -> exNine();
        }
    }

    private static void exSix() throws IOException, SQLException {

        System.out.println("Enter villain id");
        int villainId = reader.read();

        if (villainId == 0) {
            System.out.println("No such a villains was founded");
        }
        int affectedEntities = deleteMinionsByVillainId(villainId);

        String villainName = getEntityNameById(villainId, "villains");

        deleteVillainById(villainId);

        System.out.printf("%s was deleted %d minions released%n",villainName,affectedEntities);
    }

    private static void deleteVillainById(int villainId) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(
                "delete  from villains where id =? ");

        preparedStatement.setInt(1,villainId);
        preparedStatement.executeUpdate();
    }

    private static int deleteMinionsByVillainId(int villainId) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                "delete from minions_villains where villain_id = ?");
        statement.setInt(1, villainId);


        return statement.executeUpdate();
    }

    private static void exEight() throws SQLException, IOException {

        System.out.println("Enter minion id separated by space");

        int[] minionsId = Arrays.stream(reader.readLine().split("\\s+"))
                .mapToInt(Integer::parseInt)
                .toArray();

        String query = "update minions set age = age +1 where id = ?;";
        PreparedStatement statement = connection.prepareStatement(query);

        for (int i : minionsId) {
            statement.setInt(1, i);
            statement.executeUpdate();
        }
        {
            statement = connection.prepareStatement("select id,name,age from minions");
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                System.out.printf("%d %s %d%n", resultSet.getInt("id")
                        , resultSet.getString("name")
                        , resultSet.getInt("age"));
            }
        }

    }

    private static void exSeven() throws SQLException {

        PreparedStatement preparedStatement = connection.prepareStatement("select name from minions");
        ResultSet resultSet = preparedStatement.executeQuery();
        List<String> allMinionsNames = new ArrayList<>();

        while (resultSet.next()) {
            allMinionsNames.add(resultSet.getString(1));
        }
        int start = 0;
        int end = allMinionsNames.size() - 1;

        for (int i = 0; i < allMinionsNames.size(); i++) {
            System.out.println(i % 2 == 0
                    ? allMinionsNames.get(start++)
                    : allMinionsNames.get(end--));
        }
    }

    private static void exNine() throws IOException, SQLException {
        System.out.println("Enter minion id:");

        int minionId = Integer.parseInt(reader.readLine());

        String query = "call usp_get_older(?)";

        CallableStatement callableStatement = connection.prepareCall(query);
        callableStatement.setInt(1, minionId);
        callableStatement.execute();
    }

    private static void exFive() throws IOException, SQLException {
        System.out.println("Enter Country name:");

        String countryName = reader.readLine();
        String query = "update towns set name = upper(name) where country = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setString(1, countryName);

        int townsAffected = preparedStatement.executeUpdate();
        System.out.printf("%d towns name were affected.%n", townsAffected);
    }

    private static void exFour() throws IOException, SQLException {

        System.out.println("Enter minions info: name, age, town name:");
        String[] minionInfo = reader.readLine().split("\\s+");
        String minionName = minionInfo[0];
        int age = Integer.parseInt(minionInfo[1]);
        String townName = minionInfo[2];

        System.out.println("Enter villain name:");
        String villainName = reader.readLine();

        int townId = getEntityIdByName(townName, "towns");

        if (townId <= 0) {
            insertEntityInTowns(townName);
        }

        int minionId = getEntityIdByName(minionName, "minions");
        if (minionId <= 0) {
            insertMinions(minionName, townId, minionId);
            System.out.printf("Town %s was added to the database.%n", townName);

        } else {
            updateMinions(age, townId, minionId);
        }

        int villainId = getEntityIdByName(villainName, "villains");

        if (villainId <= 0) {
            String query = "insert into villains(name, evilness_factor) values (?,?);";
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setString(1, "name");
            statement.setString(2, "evil");
            statement.execute();

            System.out.printf("Villain %s was added to the table", villainName);
        }
        String query = "insert into minions_villains values(?,?);";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(1, minionId);
        statement.setInt(2, villainId);

        System.out.printf("Successfully added %s to be minion of %s%n", minionName, villainName);
    }

    private static void updateMinions(int age, int townId, int minionId) throws SQLException {
        String query = "update minions set age = ?,town_id = ? where id = ?;";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(1, age);
        statement.setInt(2, townId);
        statement.setInt(3, minionId);
    }

    private static void insertMinions(String minionName, int townId, int minionId) throws SQLException {
        String query = "insert into minions(name,age,town_id) values(?,?,?)";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, minionName);
        statement.setInt(2, minionId);
        statement.setInt(3, townId);
    }

    private static void insertEntityInTowns(String townName) throws SQLException {
        String query = "insert into towns(name) value(?)";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setString(1, townName);
        preparedStatement.execute();
    }

    private static int getEntityIdByName(String entityName, String tableName) throws SQLException {
        String query = String.format("select id from %s where name = ?", tableName);
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, entityName);
        ResultSet resultSet = statement.executeQuery();

        return resultSet.next()
                ?
                resultSet.getInt(1)
                :
                -1;
    }

    private static void exThree() throws IOException, SQLException {
        System.out.println("Enter villain id:");
        int villainId = Integer.parseInt(reader.readLine());

        String villainName = getEntityNameById(villainId, "villains");
        if (villainName == null) {
            System.out.printf("No villain with ID %d exists in the database.", villainId);
            return;
        }
        System.out.printf("Villain: %s%n", villainName);

        String query = "select  m.name,m.age from minions m\n" +
                "join minions_villains mv on m.id = mv.minion_id\n" +
                "where mv.villain_id = ?";

        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setInt(1, villainId);

        ResultSet resultSet = preparedStatement.executeQuery();
        int count = 0;
        while (resultSet.next()) {
            System.out.printf("%d. %s %d%n", ++count, resultSet.getString("name"), resultSet.getInt("age"));
        }
    }

    private static String getEntityNameById(int entityId, String tableName) throws SQLException {
        String query = String.format("select name from %s where id = ?", tableName);

        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setInt(1, entityId);

        ResultSet resultSet = preparedStatement.executeQuery();

        return resultSet.next()
                ?
                resultSet.getString("name")
                :
                null;
    }


    private static void exTwo() throws SQLException {
        PreparedStatement preparedStatement =
                connection.prepareStatement("select v.name,count(distinct mv.minion_id)as c_count from villains v\n" +
                        "join minions_villains mv on v.id = mv.villain_id\n" +
                        "group by v.name\n" +
                        "having c_count > ?;");

        preparedStatement.setInt(1, 15);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            System.out.printf("%s %d %n", resultSet.getString(1), resultSet.getInt(2));
        }
    }

    private static Connection getConnection() throws IOException, SQLException {
        System.out.println("Enter user:");
        String user = reader.readLine();
        System.out.println("Enter password");
        String password = reader.readLine();

        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);

        return DriverManager.getConnection(CONNECTION_STRING + TABLE_NAME, properties);

    }

}








