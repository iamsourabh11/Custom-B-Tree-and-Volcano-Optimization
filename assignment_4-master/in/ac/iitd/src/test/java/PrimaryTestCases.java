 import convention.PConvention;
 import rules.PRules;

 import org.apache.calcite.tools.RuleSet;
 import org.apache.calcite.tools.RuleSets;
 import org.apache.calcite.rel.RelNode;

 import org.junit.Test;
 import static org.junit.Assert.*;

 import java.util.List;
 import java.util.HashMap;
 import java.util.ArrayList;
 import java.util.Arrays;

 public class PrimaryTestCases {

     RuleSet rules = RuleSets.ofList(
         PRules.P_PROJECT_RULE,
         PRules.P_FILTER_RULE,
         PRules.P_TABLESCAN_RULE,
         PRules.P_JOIN_RULE,
         PRules.P_AGGREGATE_RULE,
         PRules.P_SORT_RULE
     );

     public RelNode createRelNode(String query, MyCalciteConnection calciteConnection) {
         try{
             RelNode relNode = calciteConnection.convertSql(calciteConnection.validateSql(calciteConnection.parseSql(query)));
             return relNode;
         }
         catch(Exception e){
             throw new RuntimeException(e);
         }
     }

     public List<Object []> eval(RelNode relNode, MyCalciteConnection calciteConnection) {
         try{

             RelNode phyRelNode = calciteConnection.logicalToPhysical(
                 relNode,
                 relNode.getTraitSet().plus(PConvention.INSTANCE),
                 rules
             );

             List<Object []> result = calciteConnection.executeQuery(phyRelNode);
             return result;
         }
         catch(Exception e){
             throw new RuntimeException(e);
         }
     }

    
     @Test
     public void testSFW() {
         try{
             MyCalciteConnection calciteConnection = new MyCalciteConnection();
             String query = "select first_name from actor where actor_id > 100 and actor_id < 150";
//             String query = "select first_name from actor";
            
             RelNode relNode = createRelNode(query, calciteConnection);
             List<Object []> result = eval(relNode, calciteConnection);
             System.out.println(result.size());
             assert(result.size() == 49);
             for(Object [] row : result){
                 assert(row.length == 1);
             }
//              sort the result
             result.sort((a, b) -> ((String)a[0]).compareTo((String)b[0]));
             System.out.println("sorting done ");

             String [] expected = new String [] {
                 "Adam",
                 "Albert",
                 "Albert",
                 "Angela",
                 "Cameron",
                 "Cate",
                 "Cate",
                 "Cuba",
                 "Dan",
                 "Daryl",
                 "Ed",
                 "Emily",
                 "Ewan",
                 "Fay",
                 "Frances",
                 "Gene",
                 "Gina",
                 "Greta",
                 "Groucho",
                 "Harrison",
                 "Jada",
                 "Jane",
                 "Julianne",
                 "Kevin",
                 "Kim",
                 "Liza",
                 "Lucille",
                 "Matthew",
                 "Morgan",
                 "Morgan",
                 "Morgan",
                 "Penelope",
                 "Penelope",
                 "Renee",
                 "Richard",
                 "Rita",
                 "River",
                 "Russell",
                 "Russell",
                 "Salma",
                 "Scarlett",
                 "Sidney",
                 "Susan",
                 "Susan",
                 "Sylvester",
                 "Walter",
                 "Warren",
                 "Warren",
                 "Whoopi"
             };

             for(int i = 0; i < result.size(); i++){
                 assert(result.get(i)[0].equals(expected[i]));
             }

//              Tip: You can use the following code to print the result and debug

              if(result == null) {
                  System.out.println("[-] No result found");
              }
              else{
                  System.out.println("[+] Final Output : ");
                  for (Object [] row : result) {
                      for (Object col : row) {
                          System.out.print(col + " ");
                      }
                      System.out.println();
                  }
              }

             calciteConnection.close();
         }
         catch(Exception e){
             System.out.println(e);
             System.out.println(e.getCause());
             fail("Exception thrown");
         }
         System.out.println("Test passed :)");
         return;
     }

//     @Test
//     public void testJoinAggregate_1() {
//         try{
//             MyCalciteConnection calciteConnection = new MyCalciteConnection();
//             String query = "SELECT COUNT(*) FROM (SELECT rental.rental_id, rental.rental_date, payment.amount FROM rental JOIN payment ON rental.rental_id = payment.rental_id)";
//
//             RelNode relNode = createRelNode(query, calciteConnection);
//             List<Object []> result = eval(relNode, calciteConnection);
//
//             assert(result.size() == 1);
//             assert(result.get(0).length == 1);
//             assert(result.get(0)[0].equals(14596));
//
//             calciteConnection.close();
//         }
//         catch(Exception e){
//             System.out.println(e);
//             System.out.println(e.getCause());
//             fail("Exception thrown");
//         }
//         System.out.println("Test passed :)");
//         return;
//     }
//
//     @Test
//     public void testJoinAggregateSort_1() {
//         try{
//             MyCalciteConnection calciteConnection = new MyCalciteConnection();
//             String query = "select film_id, count(film_id) as cnt\n"
//                 + "from rental r join inventory i\n"
//                 + "on r.inventory_id = i.inventory_id\n"
//                 + "group by film_id order by cnt DESC";
//
//             RelNode relNode = createRelNode(query, calciteConnection);
//             List<Object []> result = eval(relNode, calciteConnection);
//
//             HashMap<Integer, List<Integer>> expected = new HashMap<>();
//             expected.put(34, Arrays.asList(103));
//             expected.put(33, Arrays.asList(738));
//             expected.put(32, Arrays.asList(331, 382, 489, 730, 767));
//             expected.put(31, Arrays.asList(31, 369, 418, 621, 735, 753, 891, 973, 1000));
//             expected.put(30, Arrays.asList(109, 127, 239, 285, 341, 374, 403, 450, 559, 563, 609, 702, 748, 789, 869, 979));
//             expected.put(29, Arrays.asList(73, 86, 174, 220, 284, 301, 361, 378, 595, 849, 873, 875, 893, 941, 945));
//             expected.put(28, Arrays.asList(159, 295, 305, 356, 358, 395, 745, 764, 850, 911, 951));
//             expected.put(27, Arrays.asList(78, 114, 135, 200, 206, 228, 244, 330, 349, 367, 434, 468, 521, 525, 531, 603, 625, 638, 697, 698, 715, 835, 870, 879, 897, 958));
//             expected.put(26, Arrays.asList(12, 101, 167, 181, 234, 263, 303, 307, 391, 397, 445, 460, 471, 554, 572, 670, 683, 687, 773, 838, 890, 901, 938, 970));
//             expected.put(25, Arrays.asList(55, 154, 162, 172, 245, 247, 266, 273, 288, 309, 319, 387, 417, 545, 555, 571, 624, 644, 760, 786, 791, 856, 863, 880, 895, 902, 989));
//             expected.put(24, Arrays.asList(11, 35, 43, 45, 91, 119, 130, 131, 166, 253, 304, 366, 388, 433, 443, 447, 476, 491, 502, 527, 551, 586, 641, 649, 650, 741, 775, 790, 804, 810, 852, 865, 966));
//             expected.put(23, Arrays.asList(1, 4, 10, 51, 117, 122, 191, 199, 270, 271, 320, 327, 334, 353, 412, 437, 494, 504, 608, 614, 645, 646, 665, 676, 759, 771, 816, 843, 857, 892, 914, 915, 961, 976, 982, 995));
//             expected.put(22, Arrays.asList(15, 18, 21, 25, 26, 54, 59, 61, 79, 89, 142, 143, 176, 252, 267, 300, 314, 322, 376, 406, 408, 415, 428, 444, 500, 518, 556, 575, 651, 706, 772, 782, 800, 814, 823, 898, 949, 972, 985));
//             expected.put(21, Arrays.asList(6, 19, 22, 23, 37, 39, 49, 57, 67, 69, 83, 100, 112, 129, 132, 138, 149, 164, 193, 218, 235, 242, 249, 255, 274, 280, 292, 313, 317, 354, 377, 416, 514, 596, 602, 637, 677, 681, 727, 755, 776, 806, 807, 833, 841, 846, 861, 920, 922, 930, 953, 981));
//             expected.put(20, Arrays.asList(99, 139, 140, 147, 204, 231, 326, 350, 363, 402, 409, 414, 439, 462, 465, 467, 484, 486, 496, 501, 535, 562, 578, 579, 590, 628, 647, 668, 680, 694, 707, 720, 723, 728, 733, 743, 778, 805, 827, 845, 851, 906, 993));
//             expected.put(19, Arrays.asList(48, 72, 97, 115, 118, 150, 158, 160, 222, 227, 233, 243, 269, 281, 282, 298, 302, 311, 344, 345, 370, 396, 429, 436, 451, 457, 479, 560, 580, 610, 616, 619, 631, 663, 690, 710, 725, 734, 747, 785, 818, 864, 887, 912, 948));
//             expected.put(18, Arrays.asList(8, 17, 56, 77, 90, 116, 121, 155, 184, 212, 215, 251, 254, 286, 294, 329, 346, 383, 385, 410, 432, 456, 461, 463, 473, 474, 524, 583, 657, 688, 689, 709, 724, 746, 768, 777, 812, 871, 924, 936, 967, 971, 986));
//             expected.put(17, Arrays.asList(58, 70, 95, 169, 175, 183, 265, 275, 299, 308, 324, 333, 336, 348, 380, 398, 424, 448, 464, 481, 483, 488, 506, 510, 512, 534, 552, 561, 574, 611, 623, 626, 643, 672, 691, 693, 717, 737, 744, 763, 803, 854, 859, 877, 878, 925, 944, 964, 999));
//             expected.put(16, Arrays.asList(40, 42, 44, 50, 85, 105, 111, 123, 141, 165, 170, 179, 186, 189, 203, 226, 260, 351, 373, 392, 427, 458, 480, 526, 570, 600, 601, 606, 664, 666, 678, 679, 686, 696, 704, 708, 716, 739, 749, 757, 797, 809, 855, 862, 908, 927, 937, 956, 963, 969, 975, 980, 987, 991));
//             expected.put(15, Arrays.asList(7, 28, 68, 84, 110, 137, 152, 201, 229, 232, 236, 272, 287, 420, 421, 438, 442, 446, 452, 505, 529, 532, 542, 544, 557, 588, 589, 592, 598, 615, 618, 634, 654, 655, 732, 770, 788, 792, 795, 796, 820, 830, 844, 858, 872, 896, 907, 932, 942, 962, 988));
//             expected.put(14, Arrays.asList(16, 24, 81, 173, 190, 205, 216, 219, 241, 277, 347, 360, 365, 379, 381, 394, 426, 453, 455, 478, 493, 536, 546, 599, 620, 629, 648, 652, 658, 667, 673, 756, 774, 783, 784, 793, 832, 840, 847, 894, 900, 913, 916, 921, 946, 968, 992));
//             expected.put(13, Arrays.asList(27, 34, 63, 65, 71, 96, 98, 133, 134, 145, 151, 153, 177, 202, 209, 213, 258, 290, 293, 296, 321, 328, 423, 430, 440, 449, 487, 511, 540, 568, 573, 577, 593, 597, 604, 617, 639, 660, 661, 705, 711, 736, 762, 765, 828, 829, 842, 867, 882, 917, 929, 994));
//             expected.put(12, Arrays.asList(3, 5, 9, 80, 88, 93, 113, 194, 207, 291, 315, 323, 389, 413, 519, 533, 538, 564, 567, 576, 581, 585, 682, 766, 794, 798, 813, 821, 825, 881, 919, 928, 931, 933, 957, 977));
//             expected.put(11, Arrays.asList(46, 60, 64, 74, 75, 92, 104, 187, 210, 250, 257, 283, 306, 337, 342, 352, 469, 509, 530, 549, 587, 627, 633, 640, 692, 700, 722, 729, 761, 819, 831, 848, 853, 886, 888, 889, 899, 905, 918));
//             expected.put(10, Arrays.asList(20, 29, 66, 82, 157, 246, 276, 279, 312, 339, 368, 375, 393, 422, 477, 482, 508, 522, 528, 537, 539, 543, 553, 613, 630, 636, 662, 726, 731, 740, 750, 780, 811, 834, 837, 868, 952, 960));
//             expected.put(9, Arrays.asList(13, 30, 76, 120, 126, 188, 208, 211, 214, 230, 240, 256, 264, 338, 355, 357, 390, 425, 435, 454, 466, 492, 499, 503, 515, 516, 550, 565, 582, 591, 594, 632, 685, 714, 721, 815, 824, 826, 934, 940, 978, 983, 984, 998));
//             expected.put(8, Arrays.asList(47, 52, 124, 136, 146, 163, 197, 223, 225, 364, 371, 384, 401, 431, 490, 523, 548, 566, 656, 659, 674, 703, 718, 719, 751, 752, 758, 787, 817, 822, 866, 876, 923, 926, 939, 990));
//             expected.put(7, Arrays.asList(2, 53, 106, 156, 161, 178, 182, 185, 196, 237, 238, 248, 261, 268, 278, 289, 316, 407, 411, 470, 513, 517, 520, 541, 547, 569, 605, 622, 653, 684, 695, 779, 836, 839, 910, 935, 947, 996));
//             expected.put(6, Arrays.asList(32, 62, 102, 125, 168, 224, 259, 262, 297, 340, 372, 399, 405, 472, 475, 485, 498, 507, 635, 675, 754, 769, 799, 808, 883, 884, 885, 959, 965, 974, 997));
//             expected.put(5, Arrays.asList(94, 107, 180, 310, 335, 343, 362, 441, 459, 558, 612, 699, 781, 903));
//             expected.put(4, Arrays.asList(400, 584, 904));
//
//             assert(result.size() == 958);
//
//             HashMap<Integer, List<Integer>> actual = new HashMap<>();
//             for(Object[] row : result){
//                 int film_id = (int) row[0];
//                 int cnt = (int) row[1];
//                 if(actual.containsKey(cnt)){
//                     actual.get(cnt).add(film_id);
//                 }
//                 else{
//                     actual.put(cnt, new ArrayList<>(Arrays.asList(film_id)));
//                 }
//             }
//
//             assert(expected.size() == actual.size());
//
//             for(int key : expected.keySet()){
//                 assert(expected.get(key).size() == actual.get(key).size());
//                 expected.get(key).sort(null);
//                 for(int i = 0; i < expected.get(key).size(); i++){
//                     assert(expected.get(key).get(i).equals(actual.get(key).get(i)));
//                 }
//             }
//
//             calciteConnection.close();
//         }
//         catch(Exception e){
//             System.out.println(e);
//             System.out.println(e.getCause());
//             fail("Exception thrown");
//         }
//         System.out.println("Test passed :)");
//         return;
//     }
//
//     @Test
//     public void testSort(){
//         try{
//             MyCalciteConnection calciteConnection = new MyCalciteConnection();
//             String query = "select f.title from\n"
//                 + "(select title, length from film where rating = 'PG')\n"
//                 + "as f where f.length < 50 order by f.title DESC";
//
//             RelNode relNode = createRelNode(query, calciteConnection);
//             List<Object []> result = eval(relNode, calciteConnection);
//
//             List<String> expected = Arrays.asList("Suspects Quills", "Shanghai Tycoon", "Rush Goodfellas", "Pelican Comforts", "Iron Moon", "Hurricane Affair", "Heaven Freedom");
//
//             assert(result.size() == 7);
//             for(int i = 0; i < result.size(); i++) {
//                 assert(result.get(i).length == 1);
//                 assert(result.get(i)[0].equals(expected.get(i)));
//             }
//
//             calciteConnection.close();
//         }
//         catch(Exception e){
//             System.out.println(e);
//             System.out.println(e.getCause());
//             fail("Exception thrown");
//         }
//         System.out.println("Test passed :)");
//         return;
//     }
//
//     @Test
//     public void testBonus(){
//         try{
//             MyCalciteConnection calciteConnection = new MyCalciteConnection();
//             String query = "select first_name from actor order by first_name, last_name";
//
//             List<Object []> result = calciteConnection.executeQueryBonus(query, rules);
//
//             // print result
//             for(Object [] row : result){
//                 for(Object col : row){
//                     System.out.print(col + " ");
//                 }
//                 System.out.println();
//             }
//
//             calciteConnection.close();
//         }
//         catch(Exception e){
//             System.out.println(e);
//             System.out.println(e.getCause());
//             fail("Exception thrown");
//         }
//         System.out.println("Test passed :)");
//         return;
//     }
 }