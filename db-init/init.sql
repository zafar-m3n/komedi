-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Mar 10, 2026 at 09:24 AM
-- Server version: 10.4.28-MariaDB
-- PHP Version: 8.2.4

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `jokes_db`
--

-- --------------------------------------------------------

--
-- Table structure for table `jokes`
--

CREATE TABLE `jokes` (
  `id` int(11) NOT NULL,
  `setup` text NOT NULL,
  `punchline` text NOT NULL,
  `type_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `jokes`
--

INSERT INTO `jokes` (`id`, `setup`, `punchline`, `type_id`, `created_at`) VALUES
(1, 'Why don’t skeletons fight each other?', 'They don’t have the guts.', 1, '2026-03-10 07:34:10'),
(2, 'Why did the scarecrow win an award?', 'Because he was outstanding in his field.', 1, '2026-03-10 07:34:10'),
(3, 'Why can’t you give Elsa a balloon?', 'Because she will let it go.', 1, '2026-03-10 07:34:10'),
(4, 'Why did the bicycle fall over?', 'Because it was two tired.', 1, '2026-03-10 07:34:10'),
(5, 'What do you call fake spaghetti?', 'An impasta.', 1, '2026-03-10 07:34:10'),
(6, 'Knock knock. Who’s there? Lettuce.', 'Lettuce who? Lettuce in, it’s cold out here!', 2, '2026-03-10 07:34:10'),
(7, 'Knock knock. Who’s there? Cow says.', 'Cow says who? No silly, cow says moo!', 2, '2026-03-10 07:34:10'),
(8, 'Knock knock. Who’s there? Tank.', 'Tank who? You’re welcome.', 2, '2026-03-10 07:34:10'),
(9, 'Knock knock. Who’s there? Boo.', 'Boo who? Don’t cry, it’s just a joke.', 2, '2026-03-10 07:34:10'),
(10, 'Knock knock. Who’s there? Olive.', 'Olive who? Olive you and I miss you.', 2, '2026-03-10 07:34:10'),
(11, 'Why do programmers prefer dark mode?', 'Because light attracts bugs.', 3, '2026-03-10 07:34:10'),
(12, 'Why do Java developers wear glasses?', 'Because they don’t see sharp.', 3, '2026-03-10 07:34:10'),
(13, 'Why did the programmer quit his job?', 'Because he didn’t get arrays.', 3, '2026-03-10 07:34:10'),
(14, 'How many programmers does it take to change a light bulb?', 'None. It’s a hardware problem.', 3, '2026-03-10 07:34:10'),
(15, 'Why was the JavaScript developer sad?', 'Because he didn’t Node how to Express himself.', 3, '2026-03-10 07:34:10');

-- --------------------------------------------------------

--
-- Table structure for table `types`
--

CREATE TABLE `types` (
  `id` int(11) NOT NULL,
  `name` varchar(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Dumping data for table `types`
--

INSERT INTO `types` (`id`, `name`) VALUES
(1, 'Dad'),
(2, 'Knock Knock'),
(3, 'Programming');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `jokes`
--
ALTER TABLE `jokes`
  ADD PRIMARY KEY (`id`),
  ADD KEY `type_id` (`type_id`);

--
-- Indexes for table `types`
--
ALTER TABLE `types`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `name` (`name`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `jokes`
--
ALTER TABLE `jokes`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=16;

--
-- AUTO_INCREMENT for table `types`
--
ALTER TABLE `types`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `jokes`
--
ALTER TABLE `jokes`
  ADD CONSTRAINT `jokes_ibfk_1` FOREIGN KEY (`type_id`) REFERENCES `types` (`id`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
